---
name: generate-etl-sql
description: ETL 代码生成。根据源表 Schema、目标表 DDL 和字段映射逻辑，生成完整的 INSERT OVERWRITE SQL 脚本。支持 Hive/Impala/Doris 三引擎，具备 Window Functions、复杂 Join、Grouping Sets 等高级能力。使用场景：(1) 目标表 DDL 已就绪，需要编写加工逻辑 (2) 跨层 ETL 开发（dwd/dws → dm/da） (3) 复杂指标计算（窗口函数、多表关联、分组集） (4) 引擎迁移时 SQL 改写 (5) 已有 ETL 脚本的增量修改（新增字段、修改字段逻辑）
---

# ETL 代码生成 (Generate ETL SQL)

根据源表结构、目标表 DDL 和字段映射逻辑，生成完整的、可直接执行的 INSERT OVERWRITE SQL 脚本。

## 定位

**资深工匠** — 上游 Skill 产出"图纸"（DDL），本 Skill 负责"施工"（ETL SQL）。

## 输入输出

### 输入

| 来源 | 内容 | 必需 |
|------|------|------|
| `search-hive-metadata` | 源表 Schema（字段列表、类型、注释）、指标定义 | 是 |
| `generate-standard-ddl` | 目标表 DDL（含逻辑主键、分区、COMMENT） | 是 |
| `docs/wip/req-{name}.md` | 需求清单（指标、维度、业务逻辑描述、数据来源） | 是（支持跨会话） |
| 用户确认 | 字段映射逻辑（基于需求清单生成草案，用户补全确认） | 是 |
| `dw-requirement-triage` | 引擎选择建议（Hive/Impala/Doris） | 可选（同会话时从上下文获取） |
| 现有 ETL 脚本 | 待修改的 SQL 文件 | 是（patch 模式） |
| ALTER TABLE DDL | 已执行的 DDL 变更（新增字段定义） | 可选（patch 模式，新增字段时） |

### 输出

根据生成模式不同，输出以下文件：

**增量模式（默认）**:
- `{table_name}_etl.sql` - 日常 T+1 增量加工脚本

**初始化模式**:
- `{table_name}_etl.sql` - 日常增量脚本
- `{table_name}_init.sql` - 历史数据回刷脚本

**修改模式 (patch)**:
- 修改后的 `{table_name}_etl.sql` - 覆盖原文件
- 如有 `{table_name}_init.sql` 且变更影响 init 逻辑 → 同步更新

每个脚本包含：
- 脚本头部注释（功能、作者、调度周期、依赖表、变更记录）
- SET 参数配置（引擎级优化参数）
- INSERT OVERWRITE ... SELECT 语句
- 质量校验由 `/generate-qa-suite` 统一生成，ETL 脚本不再内置校验 SQL

---

## 生成模式选择 (Backfill Strategy)

### 模式说明

| 模式 | 用途 | 生成文件 | 分区方式 |
|------|------|---------|---------|
| **incremental** (默认) | 日常调度 | 仅增量脚本 | 静态分区 `stat_date='${stat_date}'` |
| **init** | 新表上线回刷 | 增量 + 初始化 | 动态分区 `stat_date BETWEEN` |
| **patch** | 修改已有脚本 | 修改后的完整脚本（覆盖原文件） | 保持原脚本分区方式 |

### 使用场景

#### 场景 A: 日常开发（使用默认模式）

```bash
# 仅生成增量脚本
/generate-etl-sql
```

#### 场景 B: 新表上线需要回刷历史数据

```bash
# 同时生成增量 + 初始化脚本
/generate-etl-sql --mode=init
```

用户需要在对话中明确提出："需要回刷历史数据"或"生成初始化脚本"。

#### 场景 C: 修改已有 ETL 脚本（新增字段/修改逻辑）

```bash
# 修改现有 ETL 脚本
/generate-etl-sql --mode=patch
```

用户需要提供：
1. 现有 ETL 脚本路径（或由工作流自动传入）
2. 变更需求描述（新增哪些字段、修改哪些字段的计算逻辑）
3. 如涉及新增字段，需同时提供已执行的 ALTER TABLE DDL

### 初始化脚本特性

初始化脚本与增量脚本的关键差异：

| 特性 | 增量模式 | 初始化模式 |
|------|---------|-----------|
| **分区写入** | `PARTITION (stat_date = '${stat_date}')` | `PARTITION (stat_date)` (动态) |
| **源表过滤** | `WHERE stat_date = '${stat_date}'` | `WHERE stat_date BETWEEN '${start_date}' AND '${end_date}'` |
| **GROUP BY** | `GROUP BY dims` | `GROUP BY dims, stat_date` |
| **窗口函数** | `OVER (PARTITION BY key)` | `OVER (PARTITION BY key, stat_date)` |
| **动态分区配置** | 不需要 | 必须开启（SET hive.exec.dynamic.partition） |
| **执行参数** | `hivevar stat_date` | `hivevar start_date, end_date` |

### 参数说明

初始化脚本支持以下执行方式：

```bash
# 方式 1: 指定日期范围
hive -hivevar start_date=2024-01-01 -hivevar end_date=2024-12-31 \
     -f {table_name}_init.sql

# 方式 2: Shell 计算最近 N 天
start_date=$(date -d "30 days ago" +%Y-%m-%d)
end_date=$(date -d "yesterday" +%Y-%m-%d)
hive -hivevar start_date=$start_date -hivevar end_date=$end_date \
     -f {table_name}_init.sql
```

---

## 核心工作流

```
需求文件(docs/wip/) + 源表 Schema + 目标表 DDL
    ↓
╔══════════════════════════════╗
║         Phase 4a             ║
╠──────────────────────────────╣
│ Step 0: 需求上下文加载        │
│ Inversion Gate → 映射草案    │
│ → 用户确认/补全映射逻辑      │
├──────────────────────────────┤
│ Step 1: 解析输入             │
│ 识别源表、目标表、映射关系   │
╚══════════════════════════════╝
    ↓ ⏸️ 4a 门禁（映射确认）
╔══════════════════════════════╗
║         Phase 4b             ║
╠──────────────────────────────╣
│ Step 2: 分析加工模式         │
│ 判断 ETL 复杂度与模式       │
├──────────────────────────────┤
│ Step 2.5: 逻辑流程确认       │
│ 伪代码 + 自检清单            │
╚══════════════════════════════╝
    ↓ ⏸️ 4b 门禁（逻辑计划确认）
╔══════════════════════════════╗
║         Phase 4c             ║
╠──────────────────────────────╣
│ Step 3: 构建 SQL             │
│ 选引擎语法 → 组装 SQL 块    │
├──────────────────────────────┤
│ Step 4: 优化与审查           │
│ 性能优化 + 自审              │
╚══════════════════════════════╝
    ↓ ⏸️ 4c 门禁（SQL 确认）
╔══════════════════════════════╗
║         Phase 4d             ║
╠──────────────────────────────╣
│ Step 5: 指标入库             │
│ Step 6: 血缘注册             │
╚══════════════════════════════╝
    ↓
输出完整 ETL 脚本 + 元数据注册
```

### Patch 模式流程

```
--mode=patch 流程:

现有 ETL 脚本 + 变更需求 + (可选)ALTER TABLE DDL
    ↓
┌──────────────────────────────┐
│ Step P0: 加载与解析现有脚本    │
│ 读取脚本 → 解析结构(CTE/JOIN) │
│ → 加载变更需求               │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step P1: 变更影响分析         │
│ 识别受影响的 CTE/JOIN/SELECT │
│ → 是否需要新增源表           │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step P2: 变更方案确认         │
│ 输出变更计划 → 用户确认      │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step P3: 应用变更             │
│ 修改 CTE/SELECT/JOIN        │
│ → 更新头部注释              │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 优化与审查 (复用)     │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 5/6: 指标/血缘注册 (复用)│
│ 仅处理新增/变更的指标和血缘   │
└──────────────────────────────┘
    ↓
输出修改后的完整 ETL 脚本
```

---

## Patch 模式 (--mode=patch)

> **模式定位**: 在现有 ETL 脚本基础上做增量修改（新增字段、修改字段计算逻辑、新增源表关联、移除废弃字段），输出修改后的完整脚本。

详细步骤（P0 加载解析 → P1 影响分析 → P2 方案确认 → P3 应用变更 → 流转 Step 4~6）见：
[references/patch-mode.md](references/patch-mode.md)

---

## Step 0~2: 输入解析与映射确认

> **模式分流**: 当 `--mode=patch` 时，跳过 Step 0~3，改为执行 Step P0~P3（见上方"Patch 模式"章节），然后从 Step 4 继续。以下 Step 0~3 仅适用于 incremental/init 模式。

- **Step 0**: 加载需求文件 → **信息缺口识别（Inversion Gate）** → 向用户提问收集缺口 → 生成字段映射草案 → 用户确认/补全 → 回写需求文件。**关键规则**: 有缺口先问再生成（不得用 `⚠️ 待确认` 代替提问）、不跳过确认、不从需求文档猜逻辑、迭代补全。
- **Step 1**: 解析源表（字段、粒度、角色）、目标表（主键、分区、字段分组）和映射关系。含多源消歧评分和 Plan 感知源表识别。
- **Step 2**: 根据映射复杂度自动识别加工模式（简单聚合/多表关联/窗口计算/分组集/混合），决定 CTE 拆解策略。含复杂度评估与中间表拆分建议（Step 2.1）。

详细模板、确认流程、消歧评分、CTE 命名规范见：
[references/input-analysis.md](references/input-analysis.md)

---

## Step 2.5: 逻辑流程确认 (Logic Plan Review)

**核心原则**：先思考（伪代码/逻辑流），后执行（写 SQL）。

Step 2.5 作为复杂场景门禁（窗口函数/分组集/混合模式），在生成 SQL 前必须先输出逻辑流程并完成自检。

最小执行要求：

1. 输出 Logic Plan（目标、数据源、分步处理、粒度变化）
2. 执行固定检查项（N:N 风险、分母 0、NULL、粒度收敛、JOIN 完整性等）
3. 加载 `pitfalls.md` 执行动态检查项
4. 对业务范围覆盖度与 JOIN 键歧义执行强制确认
5. 用户确认后再进入 Step 3

详细触发条件、模板、检查项与跳过规则见：
[references/logic-plan-review.md](references/logic-plan-review.md)

---

## Step 3: 构建 SQL

按以下顺序组装完整脚本：

```
[1] 脚本头部注释（功能、目标表、源表、粒度、调度、作者）
[2] SET 参数配置（Hive/Impala/Doris 各不同）
[3] INSERT OVERWRITE 语句
    [3.1] CTE 定义（WITH 子句）
    [3.2] SELECT 字段列表（维度→布尔→指标→分区，每字段一行）
    [3.3] FROM + JOIN（主表 src，维度表 dim_{实体}，每 JOIN 带注释）
    [3.4] WHERE 条件（分区过滤必须在第一行）
    [3.5] GROUP BY（完整字段名，不用列序号）
```

**关键规则速览**:
- 物理库名映射: dm→`ph_sac_dmm`, da→`ph_sac_da`, Doris→`ph_dm_sac_drs`
- 分区参数化: Hive `${hivevar:stat_date}`, Impala `${var:stat_date}`
- JOIN: 分区字段必须带上避免全表扫描，优先 `LEFT JOIN`
- SELECT: 复杂表达式换行缩进，分区字段放末尾

详细模板、SET 参数配置（三引擎）、INSERT OVERWRITE 模板、字段/JOIN/WHERE/GROUP BY 规范见：
[references/sql-construction-guide.md](references/sql-construction-guide.md)

---

## Step 4: 优化与审查

### 4.1 性能优化检查清单

在生成 SQL 后，自动检查并应用以下优化：

| 检查项 | 问题 | 优化 |
|--------|------|------|
| 分区裁剪 | WHERE 条件未包含分区字段 | 添加 `stat_date = '${stat_date}'` |
| JOIN 爆炸 | 一对多 JOIN 导致数据膨胀 | 先聚合再 JOIN，或改用子查询 |
| 数据倾斜 | GROUP BY 键分布不均 | Hive: `distribute by` / `skewjoin`；Doris: `COLOCATE` |
| MapJoin | 小表未使用 MapJoin | 添加 `/*+ MAPJOIN(dim) */` 或确认自动生效 |
| 窗口函数排序 | OVER 子句缺少 ORDER BY | 补充排序字段 |
| NULL 处理 | 聚合字段含 NULL | `COALESCE(col, 0)` 或 `IFNULL` |
| 类型转换 | JOIN 键类型不一致 | 显式 CAST |

### 4.2 数据质量校验

> **已移至 `/generate-qa-suite`**。ETL 脚本仅负责 DML 逻辑，质量校验（冒烟测试 + DQC 规则）由 generate-qa-suite 统一生成，避免重复维护。

---

## 高级能力

根据映射逻辑复杂度，自动选择 SQL 模式：

| 场景 | 关键函数/语法 | CTE 策略 |
|------|-------------|---------|
| 环比/同比 | `LAG`/`LEAD` | 独立 CTE `win_xxx`，与聚合 CTE 分离后 JOIN |
| 排名 | `ROW_NUMBER`/`RANK`/`DENSE_RANK` | 同上 |
| 累计/移动平均 | `SUM/AVG OVER (ROWS ...)` | 同上 |
| 多表关联 | CTE 拆解: `base` → `dim_xxx` → `agg_xxx` → `final` | 先聚合再 JOIN，避免膨胀 |
| Semi Join | `EXISTS` / `LEFT SEMI JOIN` (Hive/Impala) | 存在性判断，不取字段 |
| 多维组合 | `GROUPING SETS` / `CUBE` / `ROLLUP` | COALESCE 填充"全部"，配合 GROUPING_ID |

**引擎差异**: Hive `GROUPING__ID` (双下划线) vs Impala/Doris `GROUPING_ID()` (函数)

详见 [references/sql-patterns.md](references/sql-patterns.md) 获取完整 SQL 示例。

---

## 引擎与参数化

生成 SQL 前确认目标引擎，并确保脚本参数化可被调度系统注入（如 `stat_date` / `start_date` / `end_date`）。

- 引擎差异矩阵（函数、语法、变量写法）：[references/engine-syntax.md](references/engine-syntax.md)
- 增量/初始化执行示例与参数注入方式：[references/etl-examples.md](references/etl-examples.md)

---

## 完整示例

详见 [references/etl-examples.md](references/etl-examples.md)：

- 增量脚本（T+1）
- 初始化回刷脚本（动态分区）
- 增量 vs 初始化差异对照

---

## Step 5: 指标入库（闭环复用）

ETL SQL 生成后，必须执行指标复用闭环：

1. 识别候选新指标（非维度字段）
2. 调用 `search_existing_indicators` 去重
3. 用户确认注册范围（全部/部分/不注册）
4. 调用 `register_indicator` 批量写入
5. 失败时降级为 `-- [MCP-PENDING]` / `-- [REGISTER-FAILED]` 注释，不阻塞后续流程

**Patch 模式特殊处理**: 仅对**新增字段**执行指标入库检查，已有字段跳过。如字段计算逻辑变更导致口径改变，调用 `update_indicator` 更新已有指标的 `statistical_caliber` 和 `calculation_logic`。

详细规则（字段枚举、Payload 模板、失败处理、入库建议）见：
[references/metadata-registration.md](references/metadata-registration.md)

---

## Step 6: 血缘注册（自动采集）

ETL SQL 生成后，自动采集并注册血缘：

1. 解析目标表、源表与 JOIN 类型
2. （可选）解析字段级血缘与 `transform_type`
3. 调用 `register_lineage`
4. 在脚本头部输出血缘摘要
5. 失败时降级为 `-- [MCP-PENDING]` / `-- [LINEAGE-FAILED]` 注释，不阻塞 QA

**Patch 模式特殊处理**: 调用 `register_lineage` 时使用 `full_refresh=true`，因为修改后脚本的完整血缘关系可能与原脚本不同（新增/移除源表、字段映射变化），需要全量刷新。

详细规则（解析策略、transform_type 映射、Payload 模板、查询接口）见：
[references/metadata-registration.md](references/metadata-registration.md)

---

## 交互与协作

主流程中的交互确认与跨 skill 协作采用统一模板，详见：

- [references/interaction-collaboration.md](references/interaction-collaboration.md)

## References

- [references/input-analysis.md](references/input-analysis.md) - Step 0~2 输入解析与映射确认（需求加载、源表识别、多源消歧、加工模式判断）
- [references/patch-mode.md](references/patch-mode.md) - Patch 模式 Step P0~P3 详细步骤
- [references/sql-construction-guide.md](references/sql-construction-guide.md) - Step 3 SQL 构建详细模板
- [references/sql-patterns.md](references/sql-patterns.md) - 复杂 SQL 模式速查（窗口函数、Grouping Sets 等）
- [references/engine-syntax.md](references/engine-syntax.md) - Hive / Impala / Doris 语法差异与兼容性矩阵
- [references/metadata-registration.md](references/metadata-registration.md) - 指标入库与血缘注册细则（Step 5/6）
- [references/logic-plan-review.md](references/logic-plan-review.md) - Step 2.5 逻辑流程确认细则
- [references/etl-examples.md](references/etl-examples.md) - 增量/初始化执行示例
- [references/interaction-collaboration.md](references/interaction-collaboration.md) - 交互式确认与跨 skill 协作模板
