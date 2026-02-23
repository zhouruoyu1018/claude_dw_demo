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
│ td_sum_loan_amt │ SUM(src.loan_amount) │ 当日放款金额         │ ⚠️ 待确认│
│ td_cnt_loan     │ COUNT(src.loan_id)   │ 当日放款笔数         │ ⚠️ 待确认│
│ td_diff_loan_amt│ ???                  │ 日环比差值           │ ❌ 待补全│
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
| td_sum_loan_amt | SUM(src.loan_amount) WHERE loan_status='SUCCESS' | 2026-02-16 |
| td_diff_loan_amt | 当日金额 - LAG(当日金额, 1) | 2026-02-16 |
```

### 0.6 动态任务分解 (Dynamic Task Decomposition)

**触发条件**: 用户在 Phase 4 过程中明确要求拆表（如"这里需要先建一张中间表"、"这部分逻辑太复杂，拆成两步"）。

**流程**:

```
用户请求拆表
    ↓
1. 暂停当前任务
   • 保存进度到 req 文件的 phase_progress（如 phase4_etl:step2.5）
    ↓
2. 更新/创建 plan 文件
   ├─ 如果已有 plan → 新增 task-N，添加依赖边，当前任务 status → blocked
   └─ 如果此前是单表模式 → 自动升级：
      • 创建 docs/wip/plan-{project_name}.md
      • 当前任务追溯为 task-1
      • 新任务为 task-2，task-1 depends_on += task-2
    ↓
3. 创建新 req 文件
   • docs/wip/req-{new_table}.md（引导用户输入目标、粒度、关键字段）
   • front matter 含 plan、task_id 字段
    ↓
4. 执行新任务的 Phase 2→3→4→5
    ↓
5. 恢复原任务
   • 新任务完成 → 解除阻塞
   • 重新加载 req 文件
   • 将新表加入源表列表
   • 从暂停点（phase_progress）继续
```

**单表升级为多表示例**:

```
═══════════════════════════════════════════════════════════════
 ⚡ 动态分解: 单表 → 多表
═══════════════════════════════════════════════════════════════

当前任务 (da_sac_overdue_rpt) 需要一张中间聚合表。

自动创建 plan:
  task-1 (dmm_sac_overdue_agg) — 新建，即将执行
  task-2 (da_sac_overdue_rpt) — 原任务，暂停等待 task-1

先完成 task-1，再恢复 task-2。
```

### 0.5 需求变更即时同步 (Requirement Change Sync)

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
td_sum_loan_amt       ← SUM(src.loan_amount) (聚合)
td_cnt_loan           ← COUNT(src.loan_id) (聚合)
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

### 触发条件

根据 Step 2 判断的加工模式决定是否触发：

| 加工模式 | 是否触发 | 原因 |
|---------|---------|------|
| 简单聚合 | ❌ 跳过 | 逻辑直观，直接生成 SQL |
| 增量加载 | ❌ 跳过 | 模式固定 |
| 多表关联 | ⚠️ 可选 | 2 表以内跳过，3+ 表触发 |
| 窗口计算 | ✅ 触发 | 逻辑复杂，需确认 |
| 分组集 | ✅ 触发 | 维度组合需确认 |
| 混合模式 | ✅ 必须触发 | 强烈推荐 |

### 输出格式：数据流伪代码

用自然语言 + 结构化格式描述数据处理流程，每一步对应一个 CTE：

```
═══════════════════════════════════════════════════════════════
 逻辑流程 (Logic Plan) - 请确认后再生成 SQL
═══════════════════════════════════════════════════════════════

📊 目标: 计算每日各产品的放款金额、放款笔数、日环比

📥 数据源:
   • 主表: dwd.dwd_loan_detail (粒度: loan_id, stat_date)
   • 维度表: dim.dim_product (关联: product_code)

📋 处理步骤:

┌─────────────────────────────────────────────────────────────┐
│ Step 1: base (基础过滤)                                      │
├─────────────────────────────────────────────────────────────┤
│ FROM   dwd.dwd_loan_detail                                  │
│ WHERE  stat_date = '${stat_date}'                           │
│        AND loan_status = 'SUCCESS'  -- 仅成功放款            │
│ 输出   loan_id, product_code, loan_amount                   │
│ 粒度   一行 = 一笔贷款                                       │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 2: agg_today (当日聚合)                                 │
├─────────────────────────────────────────────────────────────┤
│ FROM   base                                                 │
│ GROUP BY product_code                                       │
│ SELECT product_code,                                        │
│        SUM(loan_amount)  AS td_sum_loan_amt,                │
│        COUNT(loan_id)    AS td_cnt_loan                     │
│ 粒度   一行 = 一个产品                                       │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 3: agg_yesterday (昨日聚合 - 用于环比)                  │
├─────────────────────────────────────────────────────────────┤
│ FROM   dwd.dwd_loan_detail                                  │
│ WHERE  stat_date = DATE_ADD('${stat_date}', -1)             │
│ GROUP BY product_code                                       │
│ SELECT product_code,                                        │
│        SUM(loan_amount)  AS yd_sum_loan_amt                 │
│ 粒度   一行 = 一个产品                                       │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 4: final (组装输出)                                     │
├─────────────────────────────────────────────────────────────┤
│ FROM   agg_today t                                          │
│ JOIN   dim.dim_product dim ON t.product_code = dim.product_code │
│ JOIN   agg_yesterday y ON t.product_code = y.product_code   │
│ SELECT t.product_code,                                      │
│        dim.product_name,                                    │
│        t.td_sum_loan_amt,                                   │
│        t.td_cnt_loan,                                       │
│        t.td_sum_loan_amt - COALESCE(y.yd_sum_loan_amt, 0)   │
│                                          AS td_diff_loan_amt│
│ 粒度   一行 = 一个产品 × 一天                                │
└─────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════
```

### 自检清单 (Self-Check)

在输出逻辑流后，**必须执行以下自检**并标注结果。

自检分两部分：**固定检查项**（每次必检）+ **动态检查项**（从踩坑记录加载）。

#### 固定检查项

```
🔍 自检清单（固定项）:
┌────────────────────────────────────────────────────────────┐
│ 检查项                              │ 结果   │ 备注        │
├────────────────────────────────────────────────────────────┤
│ 1. 是否有 N:N 连接导致数据膨胀?      │ ✅ 无   │ 所有 JOIN 都是 N:1 │
│ 2. 分母为 0 的情况是否处理?          │ ✅ 已处理│ 使用 COALESCE    │
│ 3. NULL 值传播是否处理?              │ ✅ 已处理│ COALESCE 兜底    │
│ 4. 粒度是否逐步收敛到目标粒度?        │ ✅ 是   │ loan_id → product_code │
│ 5. JOIN 条件是否完整（含分区过滤）?   │ ⚠️ 待确认│ dim 表是否有 stat_date 分区? │
│ 6. 窗口函数的 PARTITION/ORDER 是否正确?│ N/A    │ 本次未使用窗口函数  │
│ 7. 业务范围覆盖度是否一致?            │ ⚠️ 待确认│ 见下方详细说明      │
│ 8. JOIN 键是否存在同名/近名歧义?      │ ⚠️ 待确认│ 见下方 JOIN 键消歧  │
│ 9. JOIN 键语义类型是否匹配?           │ ⚠️ 待确认│ 见下方 JOIN 键消歧  │
└────────────────────────────────────────────────────────────┘
```

#### 检查项 7: 业务范围覆盖度校验

**目的**：需求文档定义的业务范围（如产品范围、渠道范围、地区范围等）可能与源表实际数据不一致，必须提前发现缺口并提醒用户。

**触发条件**：需求中出现以下描述时必须执行：
- "产品范围包含：XXX"
- "渠道范围：XXX"
- "仅限/包含/排除：XXX"
- 任何明确列举枚举范围的描述

**执行步骤**：

1. **提取需求范围**：从需求文档提取业务范围的枚举值列表
   - 示例：需求写"产品范围：全产品（无+车+房+易贷+消金）" → 预期 5 类产品

2. **查询源表实际值**：通过 `get_table_detail` / `list_columns` 获取源表对应字段的 COMMENT，或询问用户源表该字段的实际枚举值

3. **交叉比对**：逐项比对需求范围 vs 源表实际值
   - 需求有 & 源表有 → ✅ 覆盖
   - 需求有 & 源表无 → **⚠️ 缺口**（必须告警用户）
   - 需求无 & 源表有 → ℹ️ 提示（可能需要排除或忽略）

4. **输出覆盖度报告**：

```
🔍 业务范围覆盖度校验:
┌─────────────────────────────────────────────────────────┐
│ 范围类型: 产品范围                                        │
│ 需求定义: 全产品（无 + 车 + 房 + 易贷 + 消金）            │
│ 源表字段: dmm_sac_capital_to_m3_dtl.product_label        │
│ 源表实际: 易贷、更易贷、陆账房-无、陆易花、小橙果、陆账房-车 │
├─────────────────────────────────────────────────────────┤
│ 映射结果:                                                │
│   无   → 陆账房-无                              ✅ 覆盖  │
│   车   → 陆账房-车                              ✅ 覆盖  │
│   房   → ???                                   ⚠️ 缺口  │
│   易贷 → 易贷、更易贷                           ✅ 覆盖  │
│   消金 → 陆易花、小橙果                         ✅ 覆盖  │
├─────────────────────────────────────────────────────────┤
│ 结论: 存在 1 个缺口，需用户确认：                         │
│   1. "房" 在源表中无对应 product_label 值                 │
│      → 可能原因: 源表元数据未更新 / 产品下线 / 分类口径不同│
│      → 建议: 联系数据源负责人确认，评估是否影响指标口径    │
└─────────────────────────────────────────────────────────┘
```

**关键规则**：
- 发现缺口时**必须中断并询问用户**，不能静默跳过
- 用户确认后，在需求文件中记录确认结论（如"用户确认：房产品已下线，排除不影响口径"）
- 如果无法获取源表实际枚举值（MCP 不可用或字段 COMMENT 不含枚举信息），则向用户提问确认

#### 动态检查项（从 pitfalls.md 加载）

**执行步骤**：在开始自检前，读取 auto memory 目录下的 `pitfalls.md` 文件，检查本次 ETL 涉及的源表是否有已记录的已知问题。

```
🔍 自检清单（动态项 — 来自踩坑记录）:
┌────────────────────────────────────────────────────────────┐
│ 检查项                              │ 结果   │ 来源        │
├────────────────────────────────────────────────────────────┤
│ (从 pitfalls.md 中匹配本次涉及的     │        │             │
│  源表，逐条列出相关的已知问题)        │        │             │
│                                     │        │             │
│ 示例:                                │        │             │
│ P-001: dim_product 无分区，不应加     │ ✅ 已规避│ pitfalls.md │
│        stat_date 过滤               │        │             │
│ P-002: dwd_loan_detail.loan_amount  │ ⚠️ 待确认│ pitfalls.md │
│        含负值（退款冲正）             │        │             │
└────────────────────────────────────────────────────────────┘
```

若 `pitfalls.md` 为空或无匹配记录，输出"动态检查项：无（踩坑记录中暂无涉及本次源表的已知问题）"。

#### 错误记录（自检发现问题时）

当自检发现问题，或用户在 review 时指出 ETL 错误，**自动追加到 pitfalls.md**：

- 涉及**源表特性**（如某字段含 NULL/负值）→ 写入"源表已知特性"区
- 涉及**ETL 编写错误**（如误加分区过滤）→ 写入"ETL 常见错误"区
- 由 DQC 规则捕获的真实缺陷 → 写入"DQC 捕获的真实缺陷"区

### 自检项详解

| 检查项 | 问题场景 | 检查方法 |
|-------|---------|---------|
| **N:N 连接膨胀** | 两张表通过非唯一键 JOIN，导致行数爆炸 | 检查 JOIN 键是否为主键或唯一键 |
| **分母为 0** | 计算比率时分母可能为 0 | `CASE WHEN denom = 0 THEN NULL ELSE ... END` 或 `NULLIF` |
| **NULL 传播** | 聚合字段含 NULL，SUM 可能失真 | `COALESCE(col, 0)` 或 `IFNULL` |
| **粒度收敛** | 中间步骤粒度不明确，最终粒度错误 | 每个 CTE 标注"一行 = 什么" |
| **JOIN 分区过滤** | 维度表未按分区过滤，全表扫描 | `dim.stat_date = '${stat_date}'` 或确认维度表无分区 |
| **窗口函数边界** | `ROWS BETWEEN` 边界错误，累计值不对 | 确认 `UNBOUNDED PRECEDING` 等关键字 |
| **JOIN 键同名/近名歧义** | 源表存在多个相似字段，选错 JOIN 键 | 见下方 8 详细说明 |
| **JOIN 键语义类型不匹配** | 两侧 JOIN 键名称不同，语义也不同（如 PSID vs 编码） | 见下方 9 详细说明 |

#### 检查项 8 & 9: JOIN 键消歧 (Join Key Disambiguation)

**目的**：防止因字段名称相似或语义类型不匹配导致 JOIN 条件错误。

**检查项 8: 同名/近名字段消歧**

当选用某个字段作为 JOIN 键时，检查源表中是否存在名称相似的字段：

1. **检测方法**：对每个 JOIN 键，在源表字段列表中搜索包含相同关键词的字段
   - 示例：选用 `of_department_psid`，发现源表还有 `orphan_of_department_psid`
   - 示例：选用 `dept_id`，发现源表还有 `parent_dept_id`、`orig_dept_id`

2. **发现近名字段时**，必须中断并询问用户：

```
⚠️ JOIN 键近名歧义:
┌──────────────────────────────────────────────────────────┐
│ JOIN: dtl.of_department_psid = dim.ref_ps_dept_id        │
│                                                          │
│ 源表 dtl 中存在相似字段:                                    │
│   • of_department_psid       — [字段注释]                 │
│   • orphan_of_department_psid — [字段注释]                │
│                                                          │
│ 请确认:                                                   │
│ (A) 使用 of_department_psid                               │
│ (B) 使用 orphan_of_department_psid                        │
│ (C) 使用 COALESCE(orphan_of_department_psid,              │
│         of_department_psid)                                │
│ (D) 其他: ___                                             │
└──────────────────────────────────────────────────────────┘
```

**检查项 9: JOIN 键语义类型匹配**

验证 JOIN 两侧字段的语义类型一致（PSID ↔ PSID, 编码 ↔ 编码, ID ↔ ID）：

1. **检测方法**：从字段名称和注释推断语义类型
   - `*_psid` / `*_ps_id` → PSID（系统标识）
   - `*_code` → 业务编码
   - `*_id` → 业务 ID
   - 当两侧语义类型不一致时触发警告

2. **发现不匹配时**，必须中断并询问用户：

```
⚠️ JOIN 键语义类型不匹配:
┌──────────────────────────────────────────────────────────┐
│ JOIN: o.org_code = tgt.ref_ps_dept_id                     │
│                                                          │
│ 左侧: org_code    — 类型推断: 业务编码                     │
│ 右侧: ref_ps_dept_id — 类型推断: PSID（系统标识）          │
│                                                          │
│ 两侧语义类型不匹配，可能导致关联失败或结果错误。            │
│ 请确认正确的关联方式:                                      │
│ (A) 当前写法正确（字段名称有误导性，实际存储同类值）        │
│ (B) 右侧应改为关联维度表的对应字段                         │
│ (C) 需要通过中间维度表桥接                                 │
│ (D) 其他: ___                                             │
└──────────────────────────────────────────────────────────┘
```

**关键规则**:
- 检查项 8 和 9 发现问题时**必须中断并询问用户**，不能自行假设
- 用户确认后，在逻辑流程中标注确认结论（如"用户确认：使用 COALESCE(orphan_of_department_psid, of_department_psid)"）
- 将确认结论回写到需求文件的"字段映射（已确认）"章节

### 用户确认点

输出逻辑流 + 自检结果后，等待用户确认：

```
请确认上述逻辑是否正确？
(A) 确认无误，生成 SQL
(B) 需要修改 Step [N] 的逻辑
(C) 补充说明：___
```

### 示例：复杂场景的逻辑流

**需求**：计算每个客户的累计放款金额、放款排名、首次放款日期

```
📋 处理步骤:

Step 1: base
  └─ 过滤成功放款记录

Step 2: agg_cust
  └─ 按客户聚合: SUM(loan_amount), MIN(loan_date)

Step 3: win_rank
  └─ 窗口函数: ROW_NUMBER() OVER (ORDER BY total_loan_amt DESC)
  └─ ⚠️ 自检: ORDER BY 是否需要处理并列情况? (RANK vs ROW_NUMBER)

Step 4: win_cumsum
  └─ 窗口函数: SUM(loan_amount) OVER (PARTITION BY cust_id ORDER BY loan_date)
  └─ ⚠️ 自检: 累计是否包含当天? (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

Step 5: final
  └─ 组装输出
```

### 跳过条件

以下情况可跳过 Step 2.5，直接进入 Step 3：

1. **用户明确要求**："直接生成 SQL，不需要确认"
2. **简单聚合模式**：单表 + 单层 GROUP BY
3. **模板化 ETL**：用户提供了完整的字段映射规则

跳过时在输出中注明：

```sql
-- ============================================================
-- 逻辑流程: 跳过（简单聚合模式，单表 + GROUP BY）
-- ============================================================
```

---

## Step 3: 构建 SQL

### 3.1 脚本结构

完整脚本由以下块按顺序组成：

```
[1] 脚本头部注释
[2] SET 参数配置
[3] INSERT OVERWRITE 语句
    [3.1] CTE 定义（WITH 子句）
    [3.2] SELECT 字段列表
    [3.3] FROM + JOIN
    [3.4] WHERE 条件
    [3.5] GROUP BY
[4] （已移至 /generate-qa-suite）
```

### 3.2 脚本头部注释

```sql
-- ============================================================
-- 脚本:    {schema}/{table_name}_etl.sql
-- 功能:    {功能描述}
-- 目标表:  {schema}.{table_name}
-- 源表:    {source_table_1}, {source_table_2}, ...
-- 粒度:    {一行 = 什么}
-- 调度:    {每日/每周/每月} {T+1/实时}
-- 依赖:    {上游表或任务}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 初始创建
-- ============================================================
```

### 3.3 SET 参数配置

#### Hive (Tez)

```sql
-- === Hive 执行参数 ===
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
SET mapreduce.job.reduces=-1;

-- 按需启用（大表关联场景）
-- SET hive.auto.convert.join=true;
-- SET hive.mapjoin.smalltable.filesize=50000000;

-- 按需启用（数据倾斜场景）
-- SET hive.optimize.skewjoin=true;
-- SET hive.skewjoin.key=100000;
-- SET hive.groupby.skewindata=true;
```

#### Impala

```sql
-- === Impala 执行参数 ===
SET MEM_LIMIT=8g;
SET REQUEST_POOL='etl_pool';
-- SET COMPRESSION_CODEC='snappy';
```

#### Doris

Doris 无需 SET 参数，通过 SQL Hint 或 Session Variable 控制：
```sql
-- SET enable_vectorized_engine = true;
-- SET parallel_fragment_exec_instance_num = 8;
```

### 3.4 INSERT OVERWRITE 模板

#### Hive/Impala — 分区覆写

```sql
-- {target_schema} 按分层映射为物理库名:
--   dm 层 → ph_sac_dmm (Hive/Impala) | ph_dm_sac_drs (Doris)
--   da 层 → ph_sac_da  (Hive/Impala) | ph_dm_sac_drs (Doris)
INSERT OVERWRITE TABLE {target_schema}.{target_table}
PARTITION (stat_date)
SELECT
    -- ===== 维度字段 =====
    {dim_col_1},
    {dim_col_2},

    -- ===== 布尔字段 =====
    {bool_expression}    AS {bool_col},

    -- ===== 指标字段 =====
    {agg_expression_1}   AS {metric_col_1},
    {agg_expression_2}   AS {metric_col_2},

    -- ===== 分区字段（末尾） =====
    '${stat_date}'       AS stat_date

FROM {source_schema}.{source_table} src
LEFT JOIN {dim_schema}.{dim_table} dim
    ON src.{join_key} = dim.{join_key}
WHERE src.stat_date = '${stat_date}'
GROUP BY
    {dim_col_1},
    {dim_col_2}
;
```

#### Doris — INSERT INTO（Unique Model Upsert）

```sql
INSERT INTO {target_db}.{target_table}
SELECT
    {col_list}
FROM {source}
WHERE partition_key = '${partition_key}'
GROUP BY {group_cols}
;
```

### 3.5 SELECT 字段列表规范

字段列表中，每个字段独占一行，格式：

```sql
    {expression}    AS {target_col_name},    -- {中文注释}
```

规则：
- 维度字段在前，指标字段在后（与目标表 DDL 字段顺序一致）
- 复杂表达式换行缩进
- 分区字段放在 SELECT 最末尾
- 末尾字段无逗号

**复杂表达式换行示例：**

```sql
    CASE
        WHEN src.overdue_days BETWEEN 1 AND 30 THEN 'M1'
        WHEN src.overdue_days BETWEEN 31 AND 60 THEN 'M2'
        WHEN src.overdue_days > 60 THEN 'M3+'
        ELSE 'NORMAL'
    END                                      AS overdue_stage,       -- 逾期阶段
```

### 3.6 JOIN 规范

```sql
FROM {主表} src
-- 关联维度: 产品信息
LEFT JOIN {维度表} dim_prod
    ON src.product_code = dim_prod.product_code
    AND dim_prod.stat_date = '${stat_date}'       -- 维度表也按分区过滤
-- 关联事实: 还款信息
LEFT JOIN {事实表} repay
    ON src.loan_id = repay.loan_id
    AND repay.stat_date = '${stat_date}'
```

规范：
- 每个 JOIN 前添加注释说明关联目的
- 主表别名统一用 `src`
- 维度表别名用 `dim_{实体}`
- 事实表别名用有业务含义的缩写
- JOIN 条件中分区字段必须带上，避免全表扫描
- 优先使用 `LEFT JOIN`，仅在确认一对一时使用 `INNER JOIN`

### 3.7 WHERE 条件规范

```sql
WHERE src.stat_date = '${stat_date}'         -- 分区过滤（必须）
  AND src.is_deleted = 0             -- 逻辑删除过滤
  AND src.loan_status IN (...)       -- 业务条件
```

- 分区过滤条件**必须写在第一行**
- 使用 `${stat_date}` 参数化日期，由调度系统注入
- Hive 中使用 `${hivevar:stat_date}`，Impala 中使用 `${var:stat_date}`

### 3.8 GROUP BY 规范

```sql
GROUP BY
    src.product_code,
    src.product_name
```

- 与 SELECT 中的非聚合字段严格一致
- 不使用列序号（`GROUP BY 1, 2`），使用完整字段名
- 每个字段独占一行

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

## 引擎适配

生成 SQL 前确认目标引擎，关键差异速查：

| 差异项 | Hive | Impala | Doris |
|--------|------|--------|-------|
| 日期参数 | `${hivevar:stat_date}` | `${var:stat_date}` | `${partition_key}` |
| 覆写语法 | `INSERT OVERWRITE TABLE ... PARTITION` | 同 Hive | `INSERT INTO`（Unique Model Upsert） |
| 日期加减 | `DATE_ADD(stat_date, N)` | `DAYS_ADD(stat_date, N)` | `DATE_ADD(partition_key, INTERVAL N DAY)` |

详见 [references/engine-syntax.md](references/engine-syntax.md) 获取完整兼容性矩阵。

---

## 变量与参数化

### 标准变量

| 变量 | 含义 | Hive 写法 | Impala 写法 |
|------|------|-----------|-------------|
| `${stat_date}` | 数据日期 | `${hivevar:stat_date}` | `${var:stat_date}` |
| `${pre_date}` | 前一天 | `DATE_ADD('${hivevar:stat_date}', -1)` | `DAYS_SUB('${var:stat_date}', 1)` |
| `${month_begin}` | 月初 | `TRUNC('${hivevar:stat_date}', 'MM')` | `TRUNC('${var:stat_date}', 'MM')` |

### 调度集成

脚本需兼容调度系统的参数注入方式：

```bash
# Hive
hive -f etl_script.sql -hivevar stat_date=2026-01-27

# Impala
impala-shell -f etl_script.sql --var=stat_date=2026-01-27
```

---

## 完整示例

详见 [references/etl-examples.md](references/etl-examples.md)，包含：

- **增量脚本**: 按日+产品维度统计放款金额、放款笔数、日环比（Hive INSERT OVERWRITE + CTE）
- **初始化脚本**: 历史回刷版本（动态分区 + LAG 窗口函数替代昨日 JOIN）
- **增量 vs 初始化关键差异对比表**

---

## Step 5: 指标入库（闭环复用）

ETL SQL 生成完成后，**必须执行指标入库检查**，确保新产生的指标进入指标库供后续复用。

### 5.1 识别新指标

从生成的 ETL SQL 中提取所有目标表指标字段（非维度字段），与指标库比对：

```
目标表指标字段
    ↓
逐一调用 search_existing_indicators(指标名)
    ↓
┌─────────────┐     ┌─────────────┐
│ 已有 → 跳过  │     │ 未有 → 候选  │
└─────────────┘     └─────────────┘
```

### 5.2 询问用户

对每个候选新指标，向用户确认：

```
ETL 中发现以下新指标尚未入库：

1. 当日放款金额 (td_sum_loan_amt)
   口径: 当日所有放款订单金额之和，单位：元
   来源: ph_sac_dmm.dmm_sac_loan_prod_daily

2. 当日放款笔数 (td_cnt_loan)
   口径: 当日放款订单去重计数
   来源: ph_sac_dmm.dmm_sac_loan_prod_daily

请确认哪些需要注册为公共指标？
(A) 全部注册
(B) 仅注册第 [N] 项（逗号分隔）
(C) 全部不注册
```

### 5.3 执行入库

用户确认后，调用 `register_indicator` 批量写入。每个指标须包含完整的必填字段（含枚举约束）：

- `data_type`: 从元数据获取物理字段类型（如 `DECIMAL`/`BIGINT`/`VARCHAR` 等）
- `standard_type`: 枚举 `数值类`/`日期类`/`文本类`/`枚举类`/`时间类`
- `update_frequency`: 枚举 `实时`/`每小时`/`每日`/`每周`/`每月`/`每季`/`每年`/`手动`
- `status`: 枚举 `启用`/`未启用`/`废弃`，默认 `启用`
- `calculation_logic`（必填）: 格式 `SELECT 字段 FROM 表 WHERE 条件`

```
调用: register_indicator({
    "indicators": [
        {
            "indicator_code": "IDX_LOAN_001",
            "indicator_name": "当日放款金额",
            "indicator_english_name": "td_sum_loan_amt",
            "indicator_category": "原子指标",
            "business_domain": "贷款",
            "data_type": "DECIMAL",
            "standard_type": "数值类",
            "update_frequency": "每日",
            "status": "启用",
            "statistical_caliber": "当日所有放款订单金额之和，单位：元",
            "calculation_logic": "SELECT SUM(loan_amt) FROM dwd.dwd_loan_dtl WHERE loan_date = '${stat_date}' AND status = 'SUCCESS'",
            "data_source": "ph_sac_dmm.dmm_sac_loan_prod_daily"
        },
        ...
    ],
    "created_by": "zhangsan"
})
```

**失败处理**:

| 失败类型 | 处理方式 |
|---------|---------|
| MCP 不可用（连接失败/超时） | 将指标 JSON 输出到 ETL 脚本头部注释，标记 `-- [MCP-PENDING] register_indicator: {...}`，提示用户 MCP 恢复后补录 |
| 调用返回错误（重复编码/参数校验失败等） | 展示错误信息给用户，自动修正可修正项（如编码冲突则追加后缀重试 1 次），不可修正项标记为 `-- [REGISTER-FAILED]` 并附上错误原因 |

无论成功与否，**不阻塞后续步骤**（Step 6 血缘注册、Phase 5 测试生成照常进行）。

### 5.4 判断是否建议入库

| 条件 | 建议 |
|------|------|
| 目标表在 dm 层 | **建议入库** — dm 层指标天然可复用 |
| 目标表在 da 层 | **询问用户** — da 层可能为一次性报表 |
| 指标口径通用（放款金额/逾期率等） | **建议入库** |
| 指标口径含特殊条件（仅某产品/某渠道） | **询问用户**，入库时 remarks 注明限制条件 |

---

## Step 6: 血缘注册（自动采集）

ETL SQL 生成完成后，**自动提取并注册血缘关系**，记录目标表与源表的依赖，支持后续影响分析和数据溯源。

### 6.1 提取血缘信息

从生成的 ETL SQL 中解析：

**表级血缘**:
- 目标表: INSERT OVERWRITE/INTO 的目标表
- 源表: FROM 子句、JOIN 子句、子查询中引用的表
- JOIN 类型: FROM / LEFT JOIN / INNER JOIN / RIGHT JOIN / CROSS JOIN

**字段级血缘**（可选，按需采集）:
- 目标字段: SELECT 子句中的 AS 别名
- 源字段: 字段表达式中引用的列
- 转换类型: DIRECT / SUM / COUNT / AVG / MAX / MIN / CASE / CUSTOM

### 6.2 解析逻辑

```
生成的 ETL SQL
    ↓
解析 INSERT 目标表
    ↓
解析 FROM/JOIN 源表列表
    ↓
提取 JOIN 类型
    ↓
（可选）解析 SELECT 字段映射
    ↓
构建血缘数据结构
```

**解析示例**:

```sql
INSERT OVERWRITE TABLE ph_sac_dmm.dmm_sac_loan_prod_daily PARTITION (stat_date)
SELECT ...
FROM dwd.dwd_loan_detail src
LEFT JOIN dim.dim_product dim_prod ON ...
LEFT JOIN agg_prev ap ON ...
```

**提取结果**:

```json
{
  "target_table": "ph_sac_dmm.dmm_sac_loan_prod_daily",
  "source_tables": [
    {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
    {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
  ],
  "etl_logic_summary": "按产品维度聚合当日放款，关联产品维度表获取产品名称"
}
```

### 6.3 执行注册

生成 ETL SQL 后，**自动调用** `register_lineage`:

```
调用: register_lineage({
    "target_table": "ph_sac_dmm.dmm_sac_loan_prod_daily",
    "source_tables": [
        {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
        {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
    ],
    "etl_script_path": "sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql",
    "etl_logic_summary": "按产品维度聚合当日放款，计算日环比",
    "column_lineage": [  -- 可选
        {
            "target_column": "td_sum_loan_amt",
            "source_table": "dwd.dwd_loan_detail",
            "source_column": "loan_amount",
            "transform_type": "SUM",
            "transform_expr": "SUM(loan_amount)"
        }
    ],
    "created_by": "auto"
})
```

**失败处理**:

| 失败类型 | 处理方式 |
|---------|---------|
| MCP 不可用（连接失败/超时） | 将血缘 JSON 输出到 ETL 脚本头部注释，标记 `-- [MCP-PENDING] register_lineage: {...}`，提示用户 MCP 恢复后补录 |
| 调用返回错误（目标表不存在/参数错误等） | 展示错误信息给用户，标记为 `-- [LINEAGE-FAILED]` 并附上错误原因 |

无论成功与否，**不阻塞后续步骤**（Phase 5 测试生成照常进行）。

### 6.4 在脚本头部添加血缘注释

在生成的 ETL 脚本头部注释中，添加血缘信息摘要：

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_etl.sql
-- 功能:    加工贷款产品日维度指标宽表
-- 目标表:  ph_sac_dmm.dmm_sac_loan_prod_daily
-- ────────────────────────────────────────────────────────────
-- 数据血缘 (Data Lineage)
-- ────────────────────────────────────────────────────────────
-- 上游依赖:
--   • dwd.dwd_loan_detail (FROM) - 放款明细
--   • dim.dim_product (LEFT JOIN) - 产品维度
-- 下游影响: 查询 search_lineage_downstream 获取
-- ============================================================
```

### 6.5 血缘查询用法

在后续开发中，可通过 MCP 工具查询血缘：

**查上游（我依赖谁）**:
```
调用: search_lineage_upstream({
    "table_name": "ph_sac_dmm.dmm_sac_loan_prod_daily",
    "depth": 2,
    "include_columns": true
})
```

**查下游（谁依赖我）**:
```
调用: search_lineage_downstream({
    "table_name": "dwd.dwd_loan_detail",
    "depth": 1
})
```

### 6.6 字段级血缘采集规则

字段级血缘采集遵循以下规则：

| SELECT 表达式 | transform_type | transform_expr |
|--------------|----------------|----------------|
| `src.product_code` | `DIRECT` | `product_code` |
| `SUM(src.loan_amount)` | `SUM` | `SUM(loan_amount)` |
| `COUNT(src.loan_id)` | `COUNT` | `COUNT(loan_id)` |
| `MAX(src.amount)` | `MAX` | `MAX(amount)` |
| `AVG(src.rate)` | `AVG` | `AVG(rate)` |
| `CASE WHEN ... END` | `CASE` | 完整 CASE 表达式 |
| `a + b * c` | `CUSTOM` | 完整表达式 |

**跳过字段级采集的情况**:
- 常量字段（如 `'${stat_date}' AS stat_date`）
- 无法解析的复杂表达式
- 用户要求跳过

---

## 交互式确认

遇到以下情况时，主动询问用户：

1. **映射歧义**: "目标字段 `td_sum_loan_amt` 的来源可以是 `loan_amount` 或 `disburse_amount`，请确认应取哪个？"

2. **粒度不匹配**: "源表粒度（一笔贷款）比目标表粒度（一天一产品）更细，将使用 GROUP BY 聚合。请确认聚合维度是否正确？"

3. **增量 vs 全量**: "目标表是每日分区增量写入，还是每次全量重算？"

4. **JOIN 类型**: "源表与维度表关联可能存在匹配不上的记录，使用 LEFT JOIN 保留还是 INNER JOIN 过滤？"

5. **NULL 处理**: "指标字段在无数据时应为 0 还是 NULL？"

6. **指标入库**: "ETL 中发现 N 个新指标未入库，是否注册为公共指标？"

7. **JOIN 键近名歧义**: "源表 `dtl` 中存在 `of_department_psid` 和 `orphan_of_department_psid` 两个相似字段，应使用哪个作为 JOIN 键？还是需要 COALESCE？"

8. **JOIN 键语义不匹配**: "JOIN 左侧 `org_code` 是业务编码，右侧 `ref_ps_dept_id` 是 PSID，两者语义类型不同。是否需要通过维度表桥接？"

---

## 与其他 Skill 的协作

```
需求文档
    ↓
dw-requirement-triage            ← 需求拆解 → 字段列表 + 引擎建议
    ↓
search-hive-metadata             ← 搜索源表 + 指标复用
    ↓
generate-standard-ddl            ← 生成目标表 DDL
    ↓
generate-etl-sql                 ← 本 Skill：生成 ETL SQL
    ↓
调度上线
```

### 前置依赖

| Skill | 提供内容 |
|-------|---------|
| `dw-requirement-triage` | 需求字段列表、引擎选择建议 |
| `search-hive-metadata` | 源表 Schema、指标定义、词根 |
| `generate-standard-ddl` | 目标表 DDL（含逻辑主键、COMMENT） |

## References

- [references/sql-patterns.md](references/sql-patterns.md) - 复杂 SQL 模式速查（窗口函数、Grouping Sets、增量加载等）
- [references/engine-syntax.md](references/engine-syntax.md) - Hive / Impala / Doris 语法差异与兼容性矩阵
