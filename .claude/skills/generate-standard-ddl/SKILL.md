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

| 情况 | 条件 | 动作 |
|------|------|------|
| **CASE A: 扩列** | 找到现有表，维度相同，业务主题相近 | 生成 `ALTER TABLE ADD COLUMN` |
| **CASE B: 新建** | 未找到表，或粒度不匹配 | 生成 `CREATE TABLE` |
| **CASE C: 冲突** | 粒度相同但业务跨度大 | 询问用户 |

**判断维度匹配：**
- 比较分区键（PARTITIONED BY）
- 比较主键/维度列（非指标列：STRING/INT 类型，列名含 `_id`/`_code`/`_name`）
- 候选表维度 = 需求维度 → 匹配
- 候选表维度 ⊃ 需求维度 → 候选更细，不适合直接加列
- 候选表维度 ⊂ 需求维度 → 候选更粗，不匹配

**判断主题相近：**
- 同属贷款销售域（apply/credit/sign/loan）→ 相近
- 同属贷后管理域（repay/overdue/collect/writeoff）→ 相近
- 跨域（如 loan vs overdue）→ 不相近，走 CASE C

**输出要求：** 必须在回复中告知用户决策理由。例如：
> "检测到现有表 `dmm_sac_loan_prod_daily` 粒度与新指标一致（日+产品），建议在该表中新增字段，而不是新建表。"

---

## Step 3: 字段命名

### 3.1 语义拆分（复合词根优先）

将所有字段的业务含义拆分为语义单元，采用**复合词根优先**策略。

**核心原则**：词根库中可能同时存在最小语义单元（如"二级"→`level2`、"机构"→`org`）和复合词根（如"二级机构"→`two_level_org`）。复合词根的缩写可能与拆分后各单元拼接的结果不同，**必须优先使用复合词根**。

**执行步骤**：

1. 遍历所有需要命名的字段，提取业务含义
2. 识别字段中的**候选复合词**（2~4 字组合的业务术语，最长优先匹配），记入 `compound_candidates` 列表
   ```
   "二级机构编码" → 候选复合词: ["二级机构"]，其余: ["编码"]
   "放款金额"     → 候选复合词: ["放款金额"]，其余: []
   "营业部"       → 候选复合词: ["营业部"]
   ```
3. 将所有 `compound_candidates` 去重后，用 `search_word_root_batch` 查询（**当 batch 不可用时**，降级为并行调用 `search_word_root` 逐个查询每个复合词候选）
4. 根据查询结果分流：
   - **命中复合词根**（`match_level=exact`）→ 直接采用该复合词根，不再拆分
   - **未命中** → 拆分为最小语义单元，加入 `unique_units`
   ```
   "二级机构" → 命中 two_level_org → 直接使用，不拆分
   "放款金额" → 未命中             → 拆为 "放款" + "金额"
   "营业部"   → 未命中             → 拆为 "营业" + "部门"
   ```
5. 将拆分后的最小语义单元**合并去重**，得到 `unique_units` 列表（不含已命中复合词根的部分）

**禁止行为**：
- ❌ 跳过复合词查询，直接全部拆分为最小语义单元
- ❌ 复合词根已命中后仍拆分覆盖（如"二级机构"命中 `two_level_org` 后，不应再拆为"二级"→`level2` +"机构"→`org`）
- ❌ 搜索无结果后直接用英文翻译（如 `sales_dept`）替代 → 可能与已有词根冲突

### 3.2 并行词根查询（替代全量预取）

> ⚠️ 禁止按 tag 全量浏览词根表（词根 1000+ 条，limit=50 无法覆盖且浪费上下文）。仅查询本次字段涉及的语义单元。

将 Step 3.1 中**未被复合词根覆盖**的 `unique_units` 传入 `search_word_root_batch` 一次性查询：

```
search_word_root_batch(keywords=["放款", "金额", "逾期", "天数", ...])
```

> 注意：Step 3.1 复合词查询已命中的词根无需再查，直接从 Step 3.1 结果中复用。

**当 `search_word_root_batch` 不可用时**，降级为并行调用 `search_word_root`（每个 unique_unit 一个调用）。

**跨字段复用**：同一语义单元在多个字段中出现时（如"放款"出现在 `放款金额` 和 `放款笔数` 中），只查一次，结果跨字段复用。

合并所有返回结果构建本地查找表：`{语义单元 → [candidates]}`，并将 Step 3.1 命中的复合词根也纳入查找表。

**硬约束**：只能使用查询结果里返回的 `english_abbr` 和 `tag`，禁止自行猜测 tag 或自造缩写。

**覆盖率自检（必须执行）**：批量查询完成后，逐一核对 Step 3.1 的 `unique_units` 列表：
- 每个语义单元必须在查询结果中有对应的候选词根
- 若某个语义单元未包含在 `search_word_root_batch` 的 `keywords` 参数中 → **遗漏**，必须补查
- 若某个语义单元已查询但无结果 → 进入 Step 3.3 补查流程
- ❌ 禁止使用任何未经 `search_word_root` / `search_word_root_batch` 返回的缩写或 tag

### 3.3 补查未命中词根

仅当 Step 3.2 中某些语义单元**找不到**匹配时，才单独补查 `search_word_root(keyword="xxx", match_mode="exact_first")`。

**补查结果匹配优先级**：
1. `match_level=exact` 且 `score` 最高
2. `match_level=prefix` 且 `score` 最高
3. `match_level=fuzzy`（仅用于兜底消歧，必须在字段设计表注明）

若仍无结果，**必须阻断输出**，并明确提示用户补充/确认词根（或先入库词根）后再继续。
禁止使用通用英文缩写临时替代。

### 3.4 结构化命名草案 (field_spec)

> **前置条件检查**：进入本步骤前，确认 Step 3.1 `unique_units` 中的**每一个**语义单元都已通过 Step 3.2 或 3.3 获得了词根查询结果。若存在未查询的语义单元，返回 Step 3.2 补查，禁止跳过。

> 将语义工作和机械工作分离：模型负责语义（拆分、选根、标 tag），规则负责拼接。

对每个需要走词根流程的字段（指标/布尔/复合维度），先输出 `field_spec` 结构化数据，**禁止直接输出最终字段名**：

```
field_spec 至少包含：
- field_cn_name:   字段中文名（如"当日放款金额"）
- field_role:      dimension / metric / boolean
- semantic_units:  经 Step 3.1 复合词根优先处理后的语义单元列表（可包含复合词根，如 ["二级机构", "编码"] 或最小单元 ["当日", "汇总", "放款", "金额"]）
- selected_roots:  选中的词根缩写列表（如 ["today", "sum", "loan", "amt"]）
- selected_tags:   对应 tag 列表（如 ["TIME", "CONVERGE", "BIZ_ENTITY", "CATEGORY_WORD"]）
- root_sources:    每个词根的查询来源（如 ["batch:今天→today", "batch:汇总→sum", "batch:放款→loan", "batch:金额→amt"]）
- type_hint:       数据类型提示（如 DECIMAL(38,10)）
- comment_hint:    COMMENT 内容（如"当日放款总金额，单位：元"）
```

**root_sources 合法值**：`compound:{中文}→{缩写}`（来自 Step 3.1 复合词根查询）、`batch:{中文}→{缩写}`（来自 search_word_root_batch）、`single:{中文}→{缩写}`（来自 search_word_root 补查）。
❌ 不允许出现无来源的词根。若 root_sources 中某项无法标注查询来源，说明该词根未经查询，必须返回 Step 3.2 补查。

### 3.5 规则化拼接（确定性）

> **优先调用 `assemble_field_names` 工具**，由程序按 tag 排序拼接，消除人工排序错误。

将 Step 3.4 的 field_spec 转换为工具入参，一次性提交所有字段：

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
    },
    {
      "cn_name": "是否首次逾期",
      "units": [
        {"root": "is", "tag": "BOOL"},
        {"root": "first", "tag": "BIZ_ENTITY"},
        {"root": "overdue", "tag": "BIZ_ENTITY"}
      ]
    }
  ],
  "validate": true
})
```

工具自动按 `BOOL → TIME → CONVERGE → BIZ_ENTITY → CATEGORY_WORD` 排序后用 `_` 连接，并校验词根存在性和 tag 一致性。

**同类 tag 内部排序：** 当多个词根 tag 相同时，units 数组中按"被修饰词在前，修饰词在后"排列（工具保持同 tag 内的输入顺序）：
- `loan_amt_mtd`：loan_amt(放款金额) 是核心，mtd(月累计) 修饰它

**降级方案（`assemble_field_names` 不可用时）：** 手动按 tag 顺序拼接 `selected_roots`：

```
{BOOL}_{TIME}_{CONVERGE}_{BIZ_ENTITY}_{CATEGORY_WORD}
```

| 顺序 | tag | 说明 | 示例词根 |
|------|-----|------|----------|
| 1 | `BOOL` | 布尔前缀 | `is` |
| 2 | `TIME` | 时间范围 | `today`, `yestd`, `curr_mth`, `last_year` |
| 3 | `CONVERGE` | 聚合方式 | `sum`, `avg`, `max`, `min`, `tot`, `cum` |
| 4 | `BIZ_ENTITY` | 业务实体 | `loan`, `channel`, `overdue`, `credit` |
| 5 | `CATEGORY_WORD` | 分类词 | `amt`, `cnt`, `days`, `bal`, `fee`, `ytd`, `mtd` |

**重要：必须查词根表确认 tag，不能凭语义猜测位置。**

### 3.6 批量命名校验闸（必须执行）

> ⚠️ 默认使用批量校验 `validate_field_names`。仅当批量工具不可用时，降级为逐字段 `validate_field_name`。

所有**指标和布尔字段**命名完成后，**一次性提交到** `validate_field_names` 批量校验（维度字段走 Step 3.8 的 `{实体}_{后缀}` 模式，不进入校验闸）。**必须传 `expected_field_count`**（= Step 3.4 `field_spec` 中 metric + boolean 行数），防止模型构造 `fields` 数组时遗漏字段：

```json
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
    },
    {
      "field_name": "today_loan_cnt",
      "expected_units": [
        {"semantic_unit": "今日", "english_abbr": "today", "tag": "TIME"},
        {"semantic_unit": "放款", "english_abbr": "loan", "tag": "BIZ_ENTITY"},
        {"semantic_unit": "数量", "english_abbr": "cnt", "tag": "CATEGORY_WORD"}
      ]
    }
  ],
  "expected_field_count": 2
})
```

**批量校验检查项：**
1. 所有字段名词根存在性
2. tag 顺序合法性
3. 命名证据覆盖完整性
4. 重名/冲突字段检测
5. `expected_field_count` 与实际提交字段数是否一致（检测遗漏）

**通过标准：** `all_valid = true`

**阻断规则：**
- `all_valid = false` 时，禁止输出最终 DDL
- 只修正失败字段，只对失败字段重新校验
- 禁止整表无差别重跑

**MCP 降级：**
- 批量工具不可用时，降级为并行调用多个 `validate_field_name`
- MCP 整体不可用时，跳过校验闸门，在 DDL 头部注释标注 `-- [NAMING-UNVALIDATED] validate_field_name 未执行，待 MCP 恢复后补验`
- 输出时提醒用户 MCP 恢复后需补验

**三条红线**（必须拦截）：❌ CATEGORY_WORD 不在末尾 / ❌ TIME 不在开头 / ❌ CONVERGE 在 BIZ_ENTITY 之后。完整违规模式和正反例见 [references/naming-convention.md](references/naming-convention.md) §4

### 3.7 字段设计表（必须输出）

所有字段命名完成后，必须输出一张**字段设计表**，涵盖命名证据和字段规格：

| field_id | field_cn_name | field_role | semantic_units | selected_roots | selected_tags | final_field_name | type_hint | comment_hint | evidence_source |
|----------|--------------|------------|---------------|---------------|--------------|-----------------|-----------|-------------|----------------|
| 1 | 当日放款金额 | metric | 当日,汇总,放款,金额 | today,sum,loan,amt | TIME,CONVERGE,BIZ_ENTITY,CATEGORY_WORD | `today_sum_loan_amt` | DECIMAL(38,10) | 当日放款总金额，单位：元 | 并行词根查询 |
| 2 | 当日放款笔数 | metric | 当日,放款,数量 | today,loan,cnt | TIME,BIZ_ENTITY,CATEGORY_WORD | `today_loan_cnt` | DECIMAL(38,10) | 当日放款订单去重计数 | 并行词根查询 |

说明：
- `semantic_units` 是经 Step 3.1 复合词根优先处理后的语义单元（命中复合词根的保留整词，未命中的拆为最小单元）
- 指标/布尔字段的 `selected_tags` 必须来自 MCP 查询结果，不能凭语义猜
- **简单维度字段**（≤2 语义单元，如 `loan_id`）不进入此表，走 Step 3.8 免查流程
- **复合维度字段**（中文语义 ≥3 单元，如"机构全链路编码"）必须进入此表，与指标字段同等走词根查询流程
- `evidence_source` 记录对应工具调用方式（便于复核），合法值示例：`并行词根查询`、`单次补查`

### 3.8 维度字段命名

维度字段分两类处理：

**简单维度（免查词根）**：仅由 `{实体}_{后缀}` 构成，后缀限定为以下闭集：
`_id`, `_code`, `_name`, `_abbr`, `_date`, `_status`, `_type`, `_level`, `_flag`
- 示例：`loan_id`, `product_code`, `org_name`
- 判定条件：基于**中文业务含义**拆分后不超过 2 个语义单元（如"贷款"+"编号"= 2 个）
- ⚠️ 不要按英文字段名的 `_` 拆分来判定，复合词根（如 `two_level_org`）在英文中含多个 `_` 但在中文中是一个语义单元
- 这类字段不进入 Step 3.7 字段设计表

**复合维度（必须查词根）**：中文业务含义包含 **3 个及以上语义单元**时，必须回到 Step 3.1~3.6 走完整词根查询 + 校验流程，并进入 Step 3.7 字段设计表。
- 示例："二级机构编码" → 语义单元 [二级机构, 编码]，经 Step 3.1 复合词根查询命中 `two_level_org`，最终只有 2 个语义单元 → 简单维度
- 示例："机构全链路编码" → 语义单元 [机构, 全链路, 编码] ≥ 3 → 复合维度，必须走词根流程
- ❌ 禁止以 `field_role: dimension` 为由跳过词根查询

完整维度后缀规则见 [references/naming-convention.md](references/naming-convention.md) §3.3

### 3.9 未验证词根检测（输出前闸门）

> 在输出 DDL 前执行，防止遗漏词根查询。

**检测逻辑：**

1. 收集所有需要走词根流程的字段（指标/布尔/复合维度）的 `selected_roots` 列表，合并去重得到 `used_roots`
2. 收集所有 `root_sources` 中已标注来源的词根，得到 `validated_roots`
3. 简单维度字段（Step 3.8 免查类）不参与检测
4. 分区字段（`stat_date`, `stat_month` 等）不参与检测
5. 差集检测：`unvalidated = used_roots - validated_roots`

> 注意：不要按 `_` 拆分 `final_field_name` 来提取词根，复合词根（如 `two_level_org`）会被错误拆成多个片段导致误报。必须基于 `selected_roots` 计算。

**阻断规则：**
- `unvalidated` 非空 → 列出未验证词根及所属字段，返回 Step 3.2/3.3 补查
- ❌ 禁止跳过此检查直接输出 DDL

**示例：**
```
简单维度: "产品编码"（中文 2 语义单元）→ 不参与检测，跳过
指标字段: "当日放款金额" selected_roots: [today, sum, loan, amt] root_sources: [batch:当日→today, batch:汇总→sum, batch:放款→loan, batch:金额→amt]
指标字段: "当日逾期笔数" selected_roots: [today, overdue, cnt]  root_sources: [batch:当日→today, batch:逾期→overdue]

used_roots:      {today, sum, loan, amt, overdue, cnt}
validated_roots: {today, sum, loan, amt, overdue}
unvalidated:     {cnt} → 阻断，返回 Step 3.2/3.3 补查 "笔数/数量" 的词根
```

---

## Step 4: 字段排序与数据类型

### 4.1 字段排序规范

DDL 中字段按以下顺序排列：

```
1. 维度字段（主键/ID → 编码 → 名称 → 日期 → 状态）
2. 布尔字段（`is_` / `has_` 开头）
3. 时间类指标（`today_` → `yestd_` → `curr_mth_` → `qtr_` → `latest_{N}m_` → ...，完整顺序见 [references/naming-convention.md](references/naming-convention.md) §5）
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

- **命名校验前置**: 默认仅当所有指标和布尔字段 `validate_field_names` 通过，才允许生成 DDL；若 MCP 不可用，按 Step 3.6 的降级规则输出并加 `-- [NAMING-UNVALIDATED]` 标记
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
