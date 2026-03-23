---
name: generate-standard-ddl
description: DDL 变更设计师。凡涉及 CREATE TABLE、ALTER TABLE、分区变更、字段增删改、COMMENT/TBLPROPERTIES 调整等 DDL 变动，均触发本 Skill。Skill 内部分流为完整设计和快速变更两种模式，生成符合数仓规范的标准 DDL。
---

# 模型设计 (Generate Standard DDL)

凡涉及 DDL 变动（建表、扩列、改列、删列、分区调整、注释调整等），统一由本 Skill 处理。

## 定位

**设计师角色** — 不负责数据加工逻辑，只负责"表长什么样"。

## 输入

| 来源 | 内容 |
|------|------|
| `dw-requirement-triage` 输出 | 对齐后的需求字段列表（指标、维度、时间粒度、建议分层） |
| 用户直接请求 | 明确的表名 + DDL 变更项（如"给某表加 3 个字段"） |
| 数仓规范 | 本 Skill 内置（命名、分层、存储格式等） |
| 用户补充 | 可选：业务主题、特殊要求 |

## 输出

### 建设性变更（生成标准 DDL）
`CREATE TABLE` 或 `ALTER TABLE ADD COLUMNS` 语句，包含：
- 分区定义 (PARTITIONED BY)
- 存储格式 (STORED AS)
- 表属性 (TBLPROPERTIES)，**Hive 表必须包含逻辑主键声明**
- 每个字段和表的 COMMENT

### 破坏性变更（仅输出风险评估 + 手动指导）
删列、改类型、改分区键、重命名等高风险操作，本 Skill **不生成模板 SQL**，仅输出：
- 变更影响分析（调用 `search_lineage_downstream` 查下游依赖）
- 风险等级和缓解建议
- 手动执行的 SQL 片段参考（需用户自行确认后执行）

---

## 核心工作流

- **full_design**: Step 0 分型 → Step 1 分层表名 → Step 2 建模决策(扩列/新建) → Step 3 字段命名(语义拆分→查词根→assemble→validate) → Step 4 排序类型 → Step 5 生成 DDL
- **quick_change**: Step 0 分型 → 元数据核对+冲突检查 → Step 3(仅新字段) → Step 5 生成 DDL
- **破坏性变更**(删列/改类型/改分区键/重命名): 仅输出风险评估 + 手动指导，❌ 不生成模板 SQL

---

## Step 0: 请求分型（必须先执行）

先判断本次 DDL 请求属于哪一类，再决定后续流程：

| 模式 | 适用场景 | 后续步骤 |
|------|---------|---------|
| `full_design` | 用户给的是需求/字段清单，需要判断新建还是扩列 | Step 1 → Step 5 全流程 |
| `quick_change` | 用户已明确表名 + DDL 变更项，如"给某表增加3个字段" | 跳过 Step 1/2，仅做必要检查 |

### quick_change 模式的必要检查

即使跳过全流程，以下检查仍然必须执行：
1. **目标表存在性**：调用 `get_table_detail` 确认目标表存在
2. **同名字段冲突**：检查新增字段是否与已有字段重名
3. **分区表 CASCADE**：ALTER TABLE 分区表必须带 `CASCADE`
4. **新增指标字段命名**：新增的指标字段仍需走 Step 3 命名流程

### 高风险变更检测（quick_change 内联）

当 `quick_change` 检测到以下破坏性变更类型时，**不生成模板 SQL**，仅输出风险评估和手动指导：

| 高风险变更 | 风险说明 |
|-----------|---------|
| 删除字段 | 可能导致下游 ETL/报表失败 |
| 修改字段类型 | 可能导致数据精度丢失或转换失败 |
| 修改分区键 | 影响全部历史数据和调度任务 |
| 重命名表/字段 | 需要同步修改所有下游引用 |

风险评估输出格式：
```
⚠️ 高风险变更检测

变更类型：{变更描述}
影响范围：{调用 search_lineage_downstream 查询下游依赖}
建议：{风险缓解建议}
手动 SQL 参考：{仅供参考的 SQL 片段，需用户自行确认后执行}

⛔ 本 Skill 不自动生成破坏性变更的模板 SQL，请手动执行上述参考 SQL。
```

---

## Step 1: 确定分层与表名

> 仅 `full_design` 模式执行。`quick_change` 模式跳过此步。

### 分层选择

| 分层 | 适用场景 | Hive/Impala 库名 | Doris 库名 | 表名前缀 |
|------|---------|-----------------|-----------|----------|
| **dm** (数据集市) | 业务指标宽表，可被多个报表/接口复用 | `ph_sac_dmm` | `ph_dm_sac_drs` | `dmm_sac_` |
| **da** (数据应用) | 面向特定报表/接口的最终数据 | `ph_sac_da` | `ph_dm_sac_drs` | `da_sac_` |

> DDL 中 `{schema}` 占位符必须替换为物理库名（如 `ph_sac_dmm`），不能使用逻辑分层名（如 `dm`）。

### 表命名规则

格式: `{分层前缀}_{业务主题}_{数据粒度}[_{补充说明}]`，如 `dmm_sac_loan_prod_daily`

业务主题词、粒度后缀、完整示例详见 [references/naming-convention.md](references/naming-convention.md) §2

---

## Step 2: 建模决策 (Schema Evolution)

> 仅 `full_design` 模式执行。`quick_change` 模式跳过此步。

在生成 DDL 前，**必须执行以下检查**，决定是新建表还是扩列：

### 2.1 提取维度

从需求中提取 Group By Keys（维度列），例如：
- 需求："按日+产品+渠道统计放款金额" → 维度 = `(stat_date, product_code, channel_code)`

### 2.2 候选搜索

调用 `search_hive_metadata` 的 `search_table` 或 `search_by_comment` 搜索同主题表。

**Plan 感知搜索**: 当处于多表计划中时（req 文件含 `plan` 字段），在调用 `search_table` 前，先检查同 plan 中已完成 Phase 3 的其他任务。读取其 DDL 产出的表结构，比对粒度和主题：
- 若粒度和主题匹配 → 标记为 CASE A（扩列）候选
- 若粒度匹配但主题跨度大 → 标记为 CASE C（冲突）候选
- 目的：避免同一 plan 内重复建表

**MCP 不可用时降级**: 若 MCP Server 连接失败，询问用户是否有已知的同主题表，由用户手动提供表名后继续决策流程；若用户无法提供，默认走 CASE B（新建表）。

### 2.3 决策逻辑

**前置：表角色识别（候选表分级）**

对每张候选表，先判断其角色并分级，不同角色采用不同的决策路径：

1. 调用 `get_table_detail` 或 `list_columns` 获取候选表字段列表
2. 统计**业务列**（排除分区字段和审计字段如 `etl_time`/`create_time`/`update_time`）
3. 按以下规则判定表角色（**按优先级从高到低依次匹配，首条命中即停止**）：

| 优先级 | 表角色 | 判定规则 | 扩列优先级 |
|--------|--------|---------|-----------|
| 1 | **标签表** | 业务列 ≥ 8 且布尔/标志列占业务列比 ≥ 30% | **高** — 直接进入 CASE A 判断 |
| 2 | **维度/引用表** | 表名或表注释含 `dim`/`维度`/`关系`/`映射`/`字典`/`码表`/`参照`/`层级` | **低** — 走 CASE E 询问用户 |
| 3 | **指标宽表** | 指标列占业务列比 ≥ 20%（且未命中上述维度/引用表规则） | **高** — 直接进入 CASE A 判断 |
| 4 | **普通业务表** | 不满足以上任何一条 | **高** — 与指标宽表同等，直接进入 CASE A 判断 |

> **指标列**定义：DECIMAL/DOUBLE/BIGINT 类型且列名不含 `_id`/`_code`/`_name`/`_key`/`_level` 的字段。
>
> **布尔/标志列**定义：满足以下任一条件的字段：
> - 列名含 `is_`/`has_`/`_flag`/`_yn`/`enabled`
> - 数据类型为 BOOLEAN 或 TINYINT
>
> **判定优先级**：标签表 > 维度/引用表 > 指标宽表 > 普通业务表，首条命中即停止。
> - 即使表名含 `dim`，只要布尔/标志列占比达标且业务列 ≥ 8，仍判定为标签表（高优先级）。
> - 表名含 `dim` 等关键词时，即使指标列占比 ≥ 20%，仍判定为维度/引用表（低优先级），确保走 CASE E 询问用户。
> - 维度/引用表仅通过命名/注释关键词判定，不再作为兜底。不满足任何规则的表归为"普通业务表"，按高优先级处理。

**决策流程（两阶段）：**

```
┌─ 阶段一：CASE D 独立判断（可与阶段二同时产出）─┐
│  需要 Doris 同步表？→ 是：输出 CASE D，继续阶段二  │
│                      → 否：直接进入阶段二            │
└──────────────────────────────────────────────────────┘

┌─ 阶段二：在高/低优先级候选中，按 A → C → E → B 顺序首条命中即执行 ─┐
│  1. 高优先级候选中存在粒度匹配且主题相近的？→ CASE A（扩列）          │
│  2. 存在粒度匹配的高优先级候选，但全部跨域？→ CASE C（询问用户）      │
│  3. 低优先级候选中存在粒度匹配且主题相近的？→ CASE E（询问用户）      │
│  4. 以上均不命中？→ CASE B（新建表）                                  │
└──────────────────────────────────────────────────────────────────────┘
```

| 情况 | 条件 | 动作 |
|------|------|------|
| **CASE A: 扩列** | 高优先级候选（指标宽表/标签表/普通业务表），维度相同，业务主题相近 | 生成 `ALTER TABLE ADD COLUMN` |
| **CASE B: 新建** | 阶段二中 A/C/E 均不命中（无候选表，或粒度均不匹配） | 生成 `CREATE TABLE` |
| **CASE C: 冲突** | 存在至少一个粒度匹配的高优先级候选，且它们全部跨域（无任何主题相近的高优先级候选） | 询问用户 |
| **CASE D: 同步表** | 目标引擎 Doris，需关联 Hive 大表（≥100w），需求拆解输出同步策略以"INSERT 同步"开头 | 生成 Doris 本地同步表 `CREATE TABLE`，并标注后续需进入 generate-etl-sql 生成同步 SQL |
| **CASE E: 维度表扩列确认** | 低优先级候选（维度/引用表），维度匹配，主题相近，且无高优先级候选命中 CASE A | 询问用户：`"候选表 {table_name} 为维度/引用表，是否确认在此表上扩列？如否则新建表。"` |

**判断维度匹配：**
- 比较分区键（PARTITIONED BY）
- 比较主键/维度列（非指标列：STRING/INT 类型，列名含 `_id`/`_code`/`_name`）
- 候选表维度 = 需求维度 → 匹配
- 候选表维度 ⊃ 需求维度 → 候选更细，不适合直接加列
- 候选表维度 ⊂ 需求维度 → 候选更粗，不匹配

**判断主题相近：**
- 同属贷款销售域（apply/credit/sign/loan）→ 相近
- 同属贷后管理域（repay/overdue/collect/writeoff）→ 相近
- 跨域（如 loan vs overdue）→ 不相近

**CASE D 触发条件：**
- 需求拆解（dw-requirement-triage）输出的"同步策略"以"INSERT 同步"开头（含带过滤条件的变体如"INSERT 同步(近30天)"）
- Catalog 直查模式不需要建表，跳过 DDL 阶段
- CASE D 与 CASE A/B/C/E 独立判断，针对的是 Doris 同步目标表，不是业务指标表

**输出要求：** 必须在回复中告知用户决策理由。例如：
> "检测到现有表 `dmm_sac_loan_prod_daily` 粒度与新指标一致（日+产品），建议在该表中新增字段，而不是新建表。"

---

## Step 3: 字段命名

> 4 个子步骤：**词根查询 → 命名组装 → 校验闸 → 字段设计表**。降级策略统一见末尾。

### 3.1 词根查询

将所有字段的中文业务含义转换为词根，产出 `lookup_table = {语义单元 → (english_abbr, tag, source)}`。

**执行流程**（按顺序执行，不可跳步）：

```
第一轮：复合词根优先查询
  1. 从所有字段提取候选复合词（2~4 字业务术语，最长优先匹配）
     "二级机构编码" → 候选: ["二级机构"]，其余: ["编码"]
     "放款金额"     → 候选: ["放款金额"]
     "营业部"       → 候选: ["营业部"]
  2. search_word_root_batch(候选复合词去重列表)
  3. 分流：
     - exact 命中 → 采用复合词根，不再拆分
     - 未命中 → 拆为最小语义单元，加入 unique_units
     "二级机构" exact命中 two_level_org → 直接使用
     "放款金额" 未命中 → 拆: "放款" + "金额"
     "营业部"   未命中 → 拆: "营业" + "部门"  ← 注意拆为更小单元

第二轮：最小语义单元批量查询
  4. search_word_root_batch(unique_units 去重列表)
  5. 合并第一轮和第二轮结果，构建 lookup_table

第三轮：覆盖率检查与补查
  6. 逐一核对 unique_units：
     - 已查询且有结果 → ✅
     - 遗漏未查询 → 补查 search_word_root(keyword=X)
     - 已查询无结果 → 拆为更小单元重查 search_word_root
     - 拆到最小仍无结果 → ❌ STOP
```

> **❌ STOP**: 任何语义单元最终无词根结果时，**必须阻断**，提示用户补充词根（或先入库）后再继续。禁止使用通用英文缩写临时替代。

**匹配优先级**：`exact` > `prefix` > `fuzzy`（fuzzy 仅兜底，须在字段设计表注明）。

### 3.2 命名组装

**维度字段分流**：

- **简单维度**（中文 ≤2 语义单元 + 后缀 ∈ `{_id, _code, _name, _abbr, _date, _status, _type, _level, _flag}`）→ 仍走 Step 3.1 词根查询获取 entity 词根，但跳过 assemble/validate，直接命名 `{entity}_{suffix}`，不进入字段设计表
  - ⚠️ 按中文语义单元数判定，不按英文 `_` 拆分（复合词根如 `two_level_org` 中文是 1 个语义单元）
  - 示例："二级机构编码" → Step 3.1 复合词根查询命中 `two_level_org` → 只有 [二级机构, 编码] 2 个单元 → 简单维度 → 直接命名 `two_level_org_code`
- **复合维度**（中文 ≥3 语义单元）→ 与指标字段同等走下方流程

**指标/布尔/复合维度字段**：

对每个字段构建 `field_spec`，然后**调用 `assemble_field_names` 完成拼接**：

```
field_spec:
  field_cn_name:  "当日放款金额"
  field_role:     metric
  semantic_units: [当日, 汇总, 放款, 金额]
  selected_roots: [today, sum, loan, amt]       ← 必须来自 lookup_table
  selected_tags:  [TIME, CONVERGE, BIZ_ENTITY, CATEGORY_WORD]  ← 必须来自 lookup_table
  root_sources:   [batch:当日→today, batch:汇总→sum, batch:放款→loan, batch:金额→amt]
  type_hint:      DECIMAL(38,10)
  comment_hint:   "当日放款总金额，单位：元"
```

调用 `assemble_field_names`（一次性提交所有字段）：

```json
assemble_field_names({
  "fields": [
    {
      "cn_name": "当日放款金额",
      "units": [
        {"root": "today", "tag": "TIME"},
        {"root": "sum", "tag": "CONVERGE"},
        {"root": "loan", "tag": "BIZ_ENTITY"},
        {"root": "amt", "tag": "CATEGORY_WORD"}
      ]
    }
  ],
  "validate": true
})
```

工具自动按 `BOOL → TIME → CONVERGE → BIZ_ENTITY → CATEGORY_WORD` 排序并用 `_` 连接。

**同 tag 内部排序**：units 数组中按"被修饰词在前，修饰词在后"排列（工具保持同 tag 内输入顺序）。

> **❌ STOP**: `selected_tags` 必须来自词根查询结果（lookup_table），禁止凭语义猜测 tag。若某词根在 lookup_table 中无记录，返回 Step 3.1 补查。

**root_sources 合法值**：`compound:{中文}→{缩写}` / `batch:{中文}→{缩写}` / `single:{中文}→{缩写}`。不允许无来源词根。

### 3.3 校验闸

所有非简单维度字段命名完成后，**一次性提交** `validate_field_names` 批量校验。**必须传 `expected_field_count`**（= field_spec 中 metric + boolean + 复合维度的行数）：

```json
// 示例仅展示 1 个字段，实际应提交所有非简单维度字段
validate_field_names({
  "fields": [
    {
      "field_name": "today_sum_loan_amt",
      "expected_units": [
        {"semantic_unit": "今日", "english_abbr": "today", "tag": "TIME"},
        {"semantic_unit": "汇总", "english_abbr": "sum", "tag": "CONVERGE"},
        {"semantic_unit": "放款", "english_abbr": "loan", "tag": "BIZ_ENTITY"},
        {"semantic_unit": "金额", "english_abbr": "amt", "tag": "CATEGORY_WORD"}
      ]
    }
    // ... 其余字段省略
  ],
  "expected_field_count": 5  // 所有非简单维度字段的总数，非此示例中的 fields 数量
})
```

- `all_valid = true` → 进入 Step 3.4
- `all_valid = false` → 仅修正失败字段，仅对失败字段重新校验

> **❌ STOP**: 校验未通过时禁止输出 DDL。

### 3.4 字段设计表

按 [assets/field-design-template.md](assets/field-design-template.md) 的固定模板逐行填充，**禁止删减列**：

| # | 中文名 | 角色 | 语义单元 | 词根 | tags | 字段名 | 类型 | COMMENT | 来源 |
|---|--------|------|---------|------|------|--------|------|---------|------|
| 1 | 当日放款金额 | metric | 当日,汇总,放款,金额 | today,sum,loan,amt | TIME,CONVERGE,BIZ_ENTITY,CATEGORY_WORD | `today_sum_loan_amt` | DECIMAL(38,10) | 当日放款总金额，单位：元 | 并行词根查询 |

说明：
- 简单维度字段不进入此表
- `tags` 必须来自词根查询结果
- `来源` 合法值：`并行词根查询` / `单次补查` / `复合词根查询`

### MCP 降级（仅当工具不可用时）

| 工具 | 降级方案 |
|------|---------|
| `search_word_root_batch` | 并行调用多个 `search_word_root` |
| `assemble_field_names` | 手动按 tag 顺序拼接：`{BOOL}_{TIME}_{CONVERGE}_{BIZ_ENTITY}_{CATEGORY_WORD}`，tag 以词根查询结果为准 |
| `validate_field_names` | 并行调用多个 `validate_field_name`；若 MCP 整体不可用，DDL 头部标注 `-- [NAMING-UNVALIDATED]` 并提醒用户补验 |

---

## Step 4: 字段排序与数据类型

### 4.1 字段排序规范

DDL 中字段按以下顺序排列：

```
1. 维度字段（主键/ID → 编码 → 缩写 → 名称 → 类型 → 层级 → 日期 → 状态 → 标志）
2. 布尔字段（`is_` / `has_` 开头）
3. 时间类指标（`today_` → `yestd_` → `curr_mth_` → `qtr_` → `latest_{N}m_` → ...，完整顺序见 [references/naming-convention.md](references/naming-convention.md) §4）
4. 聚合指标（`sum_` → `tot_` → `cum_` → `avg_` → `max_` → `min_`）
5. 其他业务字段
```

### 4.2 数据类型选择

**DM/DA 层使用规范 B**（`DECIMAL(38,10)` + `STRING`），DWM/DWS/DIM 层使用规范 A（细化类型）。

分层自动识别、A/B 规范详情、类型映射见 [references/ddl-templates.md](references/ddl-templates.md) §4

---

## Step 5: 生成 DDL

DDL 模板（CREATE TABLE / ALTER TABLE）及 Impala/Doris 语法见 [references/ddl-templates.md](references/ddl-templates.md)。

### 5.0 DDL 生成前置检查

在拼装 DDL 之前，统一检查：
- 分区字段是否规范（STRING 类型、格式正确）
- `logical_primary_key` 是否完整
- ALTER 分区表是否带 `CASCADE`
- 所有字段和表是否有 COMMENT
- 字段是否重复、缺失、顺序异常

### 5.1 必须遵守的规则

- **命名校验前置**: 默认仅当所有非简单维度字段（指标 + 布尔 + 复合维度）`validate_field_names` 通过，才允许生成 DDL；若 MCP 不可用，按 Step 3 MCP 降级规则输出并加 `-- [NAMING-UNVALIDATED]` 标记
- **TBLPROPERTIES 必填**: `logical_primary_key`（逻辑主键）、`business_owner`、`data_layer`
- **ALTER TABLE 分区表必须加 `CASCADE`**: 确保新字段应用到已有分区
- **分区策略**: 日粒度用 `PARTITIONED BY (stat_date STRING)`，多维用 `(stat_date STRING, {enum_col} STRING)`
- **COMMENT**: 表注释末尾必须附加粒度声明 `[粒度:col1,col2]`

### 5.2 模式差异输出

| 模式 | 输出内容 |
|------|---------|
| `full_design` | 决策理由 + DDL + 字段设计表 |
| `quick_change` | 目标表核对结果 + DDL + 差异摘要 |

---

COMMENT 规范和完整 DDL 设计示例详见 [references/ddl-templates.md](references/ddl-templates.md) §5-6

---

## 协作与触发

- **前置**: `dw-requirement-triage`（字段列表） → `search-hive-metadata`（表搜索 / 词根查询 / 批量校验） → **本 Skill**（DDL 设计）→ ETL 开发
- **触发**: 凡涉及建表、扩列、删列、改类型、改分区键、重命名等 DDL 变动；`dw-requirement-triage` 输出后进入建模阶段；用户直接请求 DDL
- **不触发**: 仅语法问答、仅讨论字段含义、仅 ETL/DML 改动

## References

- [references/naming-convention.md](references/naming-convention.md) - 完整命名规范与词根查询指南
- [references/ddl-templates.md](references/ddl-templates.md) - Hive / Impala / Doris DDL 模板
