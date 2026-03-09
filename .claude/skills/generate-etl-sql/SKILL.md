---
name: generate-etl-sql
description: ETL 代码生成。根据源表 Schema、目标表 DDL 和字段映射逻辑，生成完整的 INSERT OVERWRITE SQL 脚本。支持 Hive/Impala/Doris 三引擎，具备 Window Functions、复杂 Join、Grouping Sets 等高级能力。使用场景：(1) 目标表 DDL 已就绪，需要编写加工逻辑 (2) 跨层 ETL 开发（dwd/dws → dm/da） (3) 复杂指标计算（窗口函数、多表关联、分组集） (4) 引擎迁移时 SQL 改写
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

### 输出

根据生成模式不同，输出以下文件：

**增量模式（默认）**:
- `{table_name}_etl.sql` - 日常 T+1 增量加工脚本

**初始化模式**:
- `{table_name}_etl.sql` - 日常增量脚本
- `{table_name}_init.sql` - 历史数据回刷脚本

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
┌──────────────────────────────┐
│ Step 0: 需求上下文加载        │
│ 读取需求 → 生成映射草案      │
│ → 用户确认/补全映射逻辑      │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 1: 解析输入             │
│ 识别源表、目标表、映射关系   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 2: 分析加工模式         │
│ 判断 ETL 复杂度与模式       │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 3: 构建 SQL             │
│ 选引擎语法 → 组装 SQL 块    │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 优化与审查           │
│ 性能优化 + 自审              │
└──────────────────────────────┘
    ↓
输出完整 ETL 脚本
```

---

## Step 0: 需求上下文加载与映射确认

ETL 开发前，必须先加载需求信息并与用户确认字段映射逻辑。

### 0.1 加载需求文件

**优先级**：
1. **同会话上下文**: 若 Phase 1 在当前会话已执行，直接从上下文获取需求清单
2. **需求文件**: 读取 `docs/wip/req-{table_name}.md`，恢复需求上下文
3. **用户手动提供**: 若以上均不可用，要求用户提供需求描述

**跨会话场景**（`--from=etl` 或新会话）:
```
扫描 docs/wip/ 目录
    ↓
├─ 找到 status: wip 的文件 → 列出供用户选择
├─ 找到多个文件 → 询问用户选择对应需求
└─ 未找到 → 要求用户提供需求描述或指标列表
    ↓
读取选中的需求文件，提取：
  • 指标列表（名称、聚合方式、业务描述）
  • 维度列表
  • 数据来源（源表建议）
  • 业务计算逻辑线索（Excel 公式、原文描述）
  • 引擎选择
```

### 0.2 生成字段映射草案

结合需求文件 + 源表 Schema + 目标表 DDL，自动生成映射草案：

```
═══════════════════════════════════════════════════════════════
 字段映射草案 (Field Mapping Draft) - 请逐项确认/补全
═══════════════════════════════════════════════════════════════

📋 需求来源: docs/wip/req-dmm_sac_loan_prod_daily.md

┌─────────────────┬──────────────────────┬────────────────────┬────────┐
│ 目标字段         │ 建议来源/计算逻辑      │ 需求描述            │ 状态    │
├─────────────────┼──────────────────────┼────────────────────┼────────┤
│ product_code    │ src.product_code     │ 产品编码（维度）     │ ✅ 自动 │
│ product_name    │ dim.product_name     │ 产品名称（维度关联） │ ✅ 自动 │
│ today_sum_loan_amt │ SUM(src.loan_amount) │ 当日放款金额         │ ⚠️ 待确认│
│ today_loan_cnt     │ COUNT(src.loan_id)   │ 当日放款笔数         │ ⚠️ 待确认│
│ today_diff_loan_amt│ ???                  │ 日环比差值           │ ❌ 待补全│
└─────────────────┴──────────────────────┴────────────────────┴────────┘

映射状态说明:
  ✅ 自动: 源表字段与目标字段名称/注释完全匹配，可自动映射
  ⚠️ 待确认: 根据需求描述推断的映射，需用户确认
  ❌ 待补全: 无法从需求和源表推断，需用户提供计算逻辑

═══════════════════════════════════════════════════════════════
```

### 0.3 用户确认映射

逐项展示映射草案，要求用户：

```
请确认或补全上述字段映射：
(A) 全部正确，继续生成 SQL
(B) 修改第 [N] 项的映射逻辑
(C) 补充缺失项的计算逻辑
(D) 有遗漏的字段需要补充

确认后我将基于最终映射生成 ETL SQL。
```

**关键规则**:
- **不跳过确认**: 即使所有字段都能自动匹配，也需用户确认"是否有遗漏"
- **不从需求文档猜逻辑**: 需求文件提供的是"业务描述"，具体的 SQL 转换逻辑（如 `SUM`/`COUNT`/`CASE WHEN`）必须由用户确认
- **迭代补全**: 用户可多轮补充，直到明确表示"全部确认"

### 0.4 映射确认后更新需求文件

用户确认的最终映射逻辑**回写到需求文件**（追加 `## 字段映射（已确认）` 章节），便于后续复查：

```markdown
## 字段映射（已确认）

| 目标字段 | 来源/计算逻辑 | 确认时间 |
|---------|-------------|---------|
| product_code | src.product_code | 2026-02-16 |
| today_sum_loan_amt | SUM(src.loan_amount) WHERE loan_status='SUCCESS' | 2026-02-16 |
| today_diff_loan_amt | 当日金额 - LAG(当日金额, 1) | 2026-02-16 |
```

### 0.5 动态任务分解 (Dynamic Task Decomposition)

> 本节仅定义 Phase 4 内的触发、暂停与恢复动作。完整编排规范（Task Registry、状态机、DAG 执行、分解请求字段）见 [multi-table-orchestration.md §6](../dw-dev-workflow/references/multi-table-orchestration.md)。

**触发条件**: 用户在 Phase 4 过程中明确要求拆表（如”这里需要先建一张中间表”、”这部分逻辑太复杂，拆成两步”）。

**流程**: 暂停当前任务（保存 `phase_progress`）→ 生成动态分解请求 → 由编排层创建/更新 plan → 执行新任务 Phase 2→5 → 恢复原任务从断点继续。

### 0.6 需求变更即时同步 (Requirement Change Sync)

**核心规则**: ETL 开发过程中（Step 0~6 任何阶段），凡涉及需求变更，**必须立即同步更新** `docs/wip/req-{table_name}.md` 对应章节，确保 wip 文件始终反映最新需求状态。

**触发场景与更新方式**:

| 变更类型 | 更新目标章节 | 示例 |
|---------|------------|------|
| 新增/移除源表 | `## 源表` + `## 关键源字段` | 用户提出需要关联新的维度表 → 追加到源表列表 |
| 调整指标口径 | `## 指标清单` 对应行 | 用户修改计算公式 → 更新口径列 |
| 新增/删除指标 | `## 指标清单` | 用户要求新增一个派生指标 → 追加行 |
| 修改维度 | `## 维度` | 用户要求增加渠道维度 → 追加维度项 |
| 补充业务逻辑 | `## 验收条件` 或 `## 业务背景` | 用户补充过滤条件或特殊处理说明 |
| 修改关键源字段 | `## 关键源字段` | 发现需要使用源表中之前未列出的字段 |

**执行规范**:
- 变更发生时**当场更新**，不等到流程结束
- 更新后在变更行末尾标注 `(Phase 4 补充, {日期})`，便于追溯
- 若变更涉及多个章节，一次性全部更新

---

## Step 1: 解析输入

### 1.1 源表识别

从 `search-hive-metadata` 获取的源表信息中提取：

- **表名**: 完整表名（如 `dwd.dwd_loan_detail`）
- **字段列表**: 名称、类型、注释
- **分区键**: 分区字段和格式
- **粒度**: 一行代表什么（如"一笔贷款一条记录"）

多源表时，标注每张表的角色：
- **主表 (Driving Table)**: 提供主键和核心维度
- **关联表 (Lookup Table)**: 提供补充字段
- **维度表 (Dim Table)**: 提供维度属性（名称、编码映射）

**Plan 感知源表识别**: 当处于多表模式（存在 plan 文件）时，源表识别额外检查同 plan 中已完成任务的 DDL 产出，作为"虚拟元数据"（表可能尚未在 metastore 中创建）。具体步骤：

1. 读取 plan 文件的 Task Registry
2. 对 status=completed 的任务，读取其 DDL 文件获取表结构
3. 这些表可作为当前任务的源表使用，无需调用 `search_table` / `get_table_detail`
4. 在 ETL 脚本头部注释中标注"源表来自同 plan 任务产出（尚未建表）"

### 1.1.1 多源消歧（当字段出现在多张表时）

当目标字段在多张候选表中都存在时，**必须应用 `search-hive-metadata` 的多源消歧策略**：

**评分优先级**: 口径一致(40) > 粒度匹配(30) > 分层优先(20) > 覆盖率(10)

**执行步骤**:

1. 调用 `search_existing_indicators` 检查指标库
   - 命中 → 直接采用 `source_table` 指定的表
   - 未命中 → 进入综合评分
   - MCP 不可用 → 跳过指标库检查，直接进入综合评分

2. 对候选表进行综合评分：
   - 粒度完全匹配 +30，需聚合 +15，更粗 +0
   - 分层：da/dm +18~20，dws +15，dim +12，dwd +8，ods +2
   - 覆盖率：(命中字段数/总字段数) × 10

3. 输出决策：
   - 分差 ≥ 10 → 自动选择最高分
   - 分差 < 10 → 询问用户确认

**在 ETL 脚本头部注释中记录决策**:

```sql
-- ============================================================
-- 数据来源决策 (Multi-Source Disambiguation)
-- ────────────────────────────────────────────────────────────
-- 字段: loan_amt
--   候选: dwd.dwd_loan_detail (23分), dws.dws_loan_daily (45分)
--   选择: dws.dws_loan_daily ✓
--   理由: 指标库命中 + 粒度完全匹配
-- ============================================================
```

### 1.2 目标表识别

从 `generate-standard-ddl` 输出的 DDL 中提取：

- **表名**: 如 `ph_sac_dmm.dmm_sac_loan_prod_daily`
- **逻辑主键**: 从 TBLPROPERTIES 的 `logical_primary_key` 获取
- **分区策略**: PARTITIONED BY 子句
- **字段列表**: 按分组排列（维度 → 布尔 → 指标）

### 1.3 映射关系

用户提供或从需求推导的字段映射，格式：

```
目标字段              ← 来源/计算逻辑
─────────────────────────────────────────────
product_code          ← src.product_code (直接映射)
product_name          ← dim.product_name (维度关联)
today_sum_loan_amt       ← SUM(src.loan_amount) (聚合)
today_loan_cnt           ← COUNT(src.loan_id) (聚合)
is_first_loan         ← CASE WHEN ... (条件计算)
his_max_overdue_days  ← MAX(...) OVER(...) (窗口函数)
```

---

## Step 2: 分析加工模式

根据映射逻辑的复杂度，自动识别加工模式：

### 模式判断矩阵

| 模式 | 特征 | SQL 结构 |
|------|------|---------|
| **简单聚合** | 单表 + GROUP BY | 单层 SELECT + GROUP BY |
| **多表关联** | 需要 JOIN 补充字段 | FROM ... JOIN ... GROUP BY |
| **窗口计算** | 需要排名、累计、环比 | 子查询/CTE + Window Functions |
| **分组集** | 多维度组合汇总 | GROUPING SETS / CUBE / ROLLUP |
| **增量加载** | 只处理新增/变更数据 | WHERE stat_date = '${stat_date}' 分区过滤 |
| **全量快照** | 每日全量重算 | 无增量条件，全分区覆盖 |
| **混合模式** | 以上组合 | CTE 分层 + 最终 JOIN 组装 |

### CTE 拆解策略

当模式为"混合模式"时，将 SQL 拆分为多个 CTE：

```sql
WITH
-- CTE 1: 基础明细（过滤+关联）
base AS ( ... ),

-- CTE 2: 聚合指标
agg_metrics AS ( ... ),

-- CTE 3: 窗口计算（环比/排名等）
window_metrics AS ( ... )

-- 最终组装
INSERT OVERWRITE TABLE ...
SELECT ...
FROM agg_metrics a
LEFT JOIN window_metrics w ON ...
```

**命名规范**:
- `base` / `base_{主题}`: 基础数据过滤
- `agg_{主题}`: 聚合计算
- `win_{主题}`: 窗口函数计算
- `dim_{实体}`: 维度关联
- `final`: 最终组装（如需要）

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

详细规则（解析策略、transform_type 映射、Payload 模板、查询接口）见：
[references/metadata-registration.md](references/metadata-registration.md)

---

## 交互与协作

主流程中的交互确认与跨 skill 协作采用统一模板，详见：

- [references/interaction-collaboration.md](references/interaction-collaboration.md)

## References

- [references/sql-construction-guide.md](references/sql-construction-guide.md) - Step 3 SQL 构建详细模板（头部注释、SET 参数、INSERT/SELECT/JOIN/WHERE/GROUP BY 规范）
- [references/sql-patterns.md](references/sql-patterns.md) - 复杂 SQL 模式速查（窗口函数、Grouping Sets、增量加载等）
- [references/engine-syntax.md](references/engine-syntax.md) - Hive / Impala / Doris 语法差异与兼容性矩阵
- [references/metadata-registration.md](references/metadata-registration.md) - 指标入库与血缘注册细则（Step 5/Step 6）
- [references/logic-plan-review.md](references/logic-plan-review.md) - Step 2.5 逻辑流程确认细则
- [references/interaction-collaboration.md](references/interaction-collaboration.md) - 交互式确认与跨 skill 协作模板
