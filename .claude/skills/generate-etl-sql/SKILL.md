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
| 用户 / 需求文档 | 字段映射逻辑（Mapping Logic） | 是 |
| `dw-requirement-triage` | 引擎选择建议（Hive/Impala/Doris） | 可选 |

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
- 数据质量校验 SQL（可选）

---

## 生成模式选择 (Backfill Strategy)

### 模式说明

| 模式 | 用途 | 生成文件 | 分区方式 |
|------|------|---------|---------|
| **incremental** (默认) | 日常调度 | 仅增量脚本 | 静态分区 `dt='${dt}'` |
| **init** | 新表上线回刷 | 增量 + 初始化 | 动态分区 `dt BETWEEN` |

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
| **分区写入** | `PARTITION (dt = '${dt}')` | `PARTITION (dt)` (动态) |
| **源表过滤** | `WHERE dt = '${dt}'` | `WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'` |
| **GROUP BY** | `GROUP BY dims` | `GROUP BY dims, dt` |
| **窗口函数** | `OVER (PARTITION BY key)` | `OVER (PARTITION BY key, dt)` |
| **动态分区配置** | 不需要 | 必须开启（SET hive.exec.dynamic.partition） |
| **执行参数** | `hivevar dt` | `hivevar start_dt, end_dt` |

### 参数说明

初始化脚本支持以下执行方式：

```bash
# 方式 1: 指定日期范围
hive -hivevar start_dt=2024-01-01 -hivevar end_dt=2024-12-31 \
     -f {table_name}_init.sql

# 方式 2: Shell 计算最近 N 天
start_dt=$(date -d "30 days ago" +%Y-%m-%d)
end_dt=$(date -d "yesterday" +%Y-%m-%d)
hive -hivevar start_dt=$start_dt -hivevar end_dt=$end_dt \
     -f {table_name}_init.sql
```

---

## 核心工作流

```
源表 Schema + 目标表 DDL + 映射逻辑
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
│ 性能优化 + 质量校验          │
└──────────────────────────────┘
    ↓
输出完整 ETL 脚本
```

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

### 1.1.1 多源消歧（当字段出现在多张表时）

当目标字段在多张候选表中都存在时，**必须应用 `search-hive-metadata` 的多源消歧策略**：

**评分优先级**: 口径一致(40) > 粒度匹配(30) > 分层优先(20) > 覆盖率(10)

**执行步骤**:

1. 调用 `search_existing_indicators` 检查指标库
   - 命中 → 直接采用 `source_table` 指定的表
   - 未命中 → 进入综合评分

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

- **表名**: 如 `dm.dmm_sac_loan_prod_daily`
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
| **增量加载** | 只处理新增/变更数据 | WHERE dt = '${dt}' 分区过滤 |
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
   • 主表: dwd.dwd_loan_detail (粒度: loan_id, dt)
   • 维度表: dim.dim_product (关联: product_code)

📋 处理步骤:

┌─────────────────────────────────────────────────────────────┐
│ Step 1: base (基础过滤)                                      │
├─────────────────────────────────────────────────────────────┤
│ FROM   dwd.dwd_loan_detail                                  │
│ WHERE  dt = '${dt}'                                         │
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
│ WHERE  dt = DATE_ADD('${dt}', -1)                           │
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

在输出逻辑流后，**必须执行以下自检**并标注结果：

```
🔍 自检清单:
┌────────────────────────────────────────────────────────────┐
│ 检查项                              │ 结果   │ 备注        │
├────────────────────────────────────────────────────────────┤
│ 1. 是否有 N:N 连接导致数据膨胀?      │ ✅ 无   │ 所有 JOIN 都是 N:1 │
│ 2. 分母为 0 的情况是否处理?          │ ✅ 已处理│ 使用 COALESCE    │
│ 3. NULL 值传播是否处理?              │ ✅ 已处理│ COALESCE 兜底    │
│ 4. 粒度是否逐步收敛到目标粒度?        │ ✅ 是   │ loan_id → product_code │
│ 5. JOIN 条件是否完整（含分区过滤）?   │ ⚠️ 待确认│ dim 表是否有 dt 分区? │
│ 6. 窗口函数的 PARTITION/ORDER 是否正确?│ N/A    │ 本次未使用窗口函数  │
└────────────────────────────────────────────────────────────┘
```

### 自检项详解

| 检查项 | 问题场景 | 检查方法 |
|-------|---------|---------|
| **N:N 连接膨胀** | 两张表通过非唯一键 JOIN，导致行数爆炸 | 检查 JOIN 键是否为主键或唯一键 |
| **分母为 0** | 计算比率时分母可能为 0 | `CASE WHEN denom = 0 THEN NULL ELSE ... END` 或 `NULLIF` |
| **NULL 传播** | 聚合字段含 NULL，SUM 可能失真 | `COALESCE(col, 0)` 或 `IFNULL` |
| **粒度收敛** | 中间步骤粒度不明确，最终粒度错误 | 每个 CTE 标注"一行 = 什么" |
| **JOIN 分区过滤** | 维度表未按分区过滤，全表扫描 | `dim.dt = '${dt}'` 或确认维度表无分区 |
| **窗口函数边界** | `ROWS BETWEEN` 边界错误，累计值不对 | 确认 `UNBOUNDED PRECEDING` 等关键字 |

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
[4] 数据质量校验（可选）
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
INSERT OVERWRITE TABLE {target_schema}.{target_table}
PARTITION (dt)
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
    '{$dt}'              AS dt

FROM {source_schema}.{source_table} src
LEFT JOIN {dim_schema}.{dim_table} dim
    ON src.{join_key} = dim.{join_key}
WHERE src.dt = '${dt}'
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
WHERE dt = '${dt}'
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
    AND dim_prod.dt = '${dt}'       -- 维度表也按分区过滤
-- 关联事实: 还款信息
LEFT JOIN {事实表} repay
    ON src.loan_id = repay.loan_id
    AND repay.dt = '${dt}'
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
WHERE src.dt = '${dt}'               -- 分区过滤（必须）
  AND src.is_deleted = 0             -- 逻辑删除过滤
  AND src.loan_status IN (...)       -- 业务条件
```

- 分区过滤条件**必须写在第一行**
- 使用 `${dt}` 参数化日期，由调度系统注入
- Hive 中使用 `${hivevar:dt}`，Impala 中使用 `${var:dt}`

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
| 分区裁剪 | WHERE 条件未包含分区字段 | 添加 `dt = '${dt}'` |
| JOIN 爆炸 | 一对多 JOIN 导致数据膨胀 | 先聚合再 JOIN，或改用子查询 |
| 数据倾斜 | GROUP BY 键分布不均 | Hive: `distribute by` / `skewjoin`；Doris: `COLOCATE` |
| MapJoin | 小表未使用 MapJoin | 添加 `/*+ MAPJOIN(dim) */` 或确认自动生效 |
| 窗口函数排序 | OVER 子句缺少 ORDER BY | 补充排序字段 |
| NULL 处理 | 聚合字段含 NULL | `COALESCE(col, 0)` 或 `IFNULL` |
| 类型转换 | JOIN 键类型不一致 | 显式 CAST |

### 4.2 数据质量校验 SQL（可选输出）

在主 ETL 脚本后，附加校验 SQL：

```sql
-- ============================================================
-- 数据质量校验
-- ============================================================

-- 1. 行数校验：目标表 vs 源表
SELECT '目标行数' AS check_item, COUNT(*) AS cnt
FROM {target_table} WHERE dt = '${dt}'
UNION ALL
SELECT '源表行数', COUNT(*)
FROM {source_table} WHERE dt = '${dt}';

-- 2. 主键唯一性校验
SELECT '主键重复数' AS check_item, COUNT(*) AS cnt
FROM (
    SELECT {pk_cols}, COUNT(*) AS dup_cnt
    FROM {target_table}
    WHERE dt = '${dt}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) t;

-- 3. NULL 值校验（关键字段）
SELECT '关键字段NULL数' AS check_item, COUNT(*) AS cnt
FROM {target_table}
WHERE dt = '${dt}'
  AND ({key_col_1} IS NULL OR {key_col_2} IS NULL);
```

---

## 高级能力

### Window Functions（窗口函数）

支持场景及对应模式：

| 场景 | 函数 | 示例 |
|------|------|------|
| 环比/同比 | `LAG` / `LEAD` | `LAG(amt, 1) OVER (PARTITION BY prod ORDER BY dt)` |
| 排名 | `ROW_NUMBER` / `RANK` / `DENSE_RANK` | `ROW_NUMBER() OVER (PARTITION BY cust ORDER BY amt DESC)` |
| 累计值 | `SUM(...) OVER (... ROWS UNBOUNDED PRECEDING)` | 累计放款金额 |
| 移动平均 | `AVG(...) OVER (... ROWS N PRECEDING)` | 7日移动平均 |
| 首末值 | `FIRST_VALUE` / `LAST_VALUE` | 首次放款日期 |

**CTE 拆解模式：** 窗口函数计算放在独立 CTE 中，与聚合 CTE 分离后 JOIN：

```sql
WITH
agg AS (
    SELECT product_code, dt,
           SUM(loan_amt) AS td_sum_loan_amt
    FROM dwd.dwd_loan_detail
    WHERE dt = '${dt}'
    GROUP BY product_code, dt
),
win AS (
    SELECT product_code, dt,
           td_sum_loan_amt,
           LAG(td_sum_loan_amt, 1) OVER (
               PARTITION BY product_code ORDER BY dt
           ) AS yd_sum_loan_amt
    FROM agg
)
INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily PARTITION (dt)
SELECT
    w.product_code,
    w.td_sum_loan_amt,
    w.td_sum_loan_amt - COALESCE(w.yd_sum_loan_amt, 0)
                                         AS td_diff_loan_amt,  -- 日环比差值
    w.dt
FROM win w;
```

### 复杂 JOIN

#### 多表关联组装

```sql
WITH
-- 主维度: 当日放款明细
base AS (
    SELECT loan_id, product_code, channel_code, loan_amount, cust_id
    FROM dwd.dwd_loan_detail
    WHERE dt = '${dt}'
),
-- 关联: 客户维度
dim_cust AS (
    SELECT cust_id, cust_name, cust_level
    FROM dim.dim_customer
    WHERE dt = '${dt}'
),
-- 关联: 历史逾期
his_overdue AS (
    SELECT loan_id, MAX(overdue_days) AS his_max_overdue_days
    FROM dwd.dwd_overdue_detail
    WHERE dt <= '${dt}'
    GROUP BY loan_id
)
SELECT
    b.product_code,
    dc.cust_level,
    SUM(b.loan_amount)                   AS td_sum_loan_amt,
    MAX(ho.his_max_overdue_days)         AS his_max_overdue_days,
    '${dt}'                              AS dt
FROM base b
LEFT JOIN dim_cust dc ON b.cust_id = dc.cust_id
LEFT JOIN his_overdue ho ON b.loan_id = ho.loan_id
GROUP BY b.product_code, dc.cust_level;
```

#### Semi Join（存在性判断）

```sql
-- 判断"是否有逾期记录"，不需要取逾期字段
WHERE EXISTS (
    SELECT 1 FROM dwd.dwd_overdue_detail od
    WHERE od.loan_id = src.loan_id
      AND od.overdue_days > 0
)
-- 或使用 LEFT SEMI JOIN (Hive/Impala)
LEFT SEMI JOIN dwd.dwd_overdue_detail od
    ON src.loan_id = od.loan_id AND od.overdue_days > 0
```

### Grouping Sets / CUBE / ROLLUP

#### 多维度组合汇总

```sql
INSERT OVERWRITE TABLE dm.dmm_sac_loan_multi_dim PARTITION (dt)
SELECT
    COALESCE(product_code, '全部')       AS product_code,
    COALESCE(channel_code, '全部')       AS channel_code,
    SUM(loan_amount)                     AS td_sum_loan_amt,
    COUNT(loan_id)                       AS td_cnt_loan,
    GROUPING__ID                         AS grouping_id,          -- Hive 专用
    '${dt}'                              AS dt
FROM dwd.dwd_loan_detail
WHERE dt = '${dt}'
GROUP BY product_code, channel_code
GROUPING SETS (
    (product_code, channel_code),   -- 产品+渠道
    (product_code),                 -- 仅产品
    (channel_code),                 -- 仅渠道
    ()                              -- 全局汇总
);
```

**引擎差异注意：**
- Hive: 使用 `GROUPING__ID`（双下划线）
- Impala: 使用 `GROUPING_ID()`（函数调用）
- Doris: 使用 `GROUPING_ID()` 或 `GROUPING(col)`

---

## 引擎适配

生成 SQL 前确认目标引擎，适配语法差异：

| 特性 | Hive | Impala | Doris |
|------|------|--------|-------|
| 覆写语法 | `INSERT OVERWRITE TABLE ... PARTITION` | `INSERT OVERWRITE ... PARTITION` | `INSERT INTO`（Unique Model 自动 Upsert） |
| 日期参数 | `${hivevar:dt}` | `${var:dt}` | 通过应用层传参或硬编码 |
| GROUPING ID | `GROUPING__ID` | `GROUPING_ID()` | `GROUPING_ID()` |
| NVL | `NVL()` 或 `COALESCE()` | `IFNULL()` 或 `COALESCE()` | `IFNULL()` 或 `COALESCE()` |
| 字符串拼接 | `CONCAT()` | `CONCAT()` 或 `\|\|` | `CONCAT()` |
| 日期加减 | `DATE_ADD(dt, N)` | `DAYS_ADD(dt, N)` | `DATE_ADD(dt, INTERVAL N DAY)` |
| 日期差 | `DATEDIFF(d1, d2)` | `DATEDIFF(d1, d2)` | `DATEDIFF(d1, d2)` |
| 类型转换 | `CAST(x AS TYPE)` | `CAST(x AS TYPE)` | `CAST(x AS TYPE)` |
| CTE | 支持 | 支持 | 支持 |
| Window Functions | 完整支持 | 完整支持 | 完整支持 |

详见 [references/engine-syntax.md](references/engine-syntax.md)。

---

## 变量与参数化

### 标准变量

| 变量 | 含义 | Hive 写法 | Impala 写法 |
|------|------|-----------|-------------|
| `${dt}` | 数据日期 | `${hivevar:dt}` | `${var:dt}` |
| `${pre_dt}` | 前一天 | `DATE_ADD('${hivevar:dt}', -1)` | `DAYS_SUB('${var:dt}', 1)` |
| `${month_begin}` | 月初 | `TRUNC('${hivevar:dt}', 'MM')` | `TRUNC('${var:dt}', 'MM')` |

### 调度集成

脚本需兼容调度系统的参数注入方式：

```bash
# Hive
hive -f etl_script.sql -hivevar dt=2026-01-27

# Impala
impala-shell -f etl_script.sql --var=dt=2026-01-27
```

---

## 完整示例

**需求：** 按日+产品维度统计放款金额、放款笔数、日环比放款金额

**源表：** `dwd.dwd_loan_detail` (loan_id, product_code, loan_amount, loan_date, dt)

**目标表：** `dm.dmm_sac_loan_prod_daily` (product_code, product_name, td_sum_loan_amt, td_cnt_loan, td_diff_loan_amt, dt)

**生成脚本：**

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_etl.sql
-- 功能:    加工贷款产品日维度指标宽表
-- 目标表:  dm.dmm_sac_loan_prod_daily
-- 源表:    dwd.dwd_loan_detail, dim.dim_product
-- 粒度:    一行 = 一天 × 一产品
-- 调度:    每日 T+1
-- 依赖:    dwd.dwd_loan_detail (dt=${dt}), dim.dim_product
-- 作者:    auto-generated
-- 创建日期: 2026-01-27
-- 修改记录:
--   2026-01-27 auto-generated 初始创建
-- ============================================================

-- === Hive 执行参数 ===
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

-- === ETL 主逻辑 ===
WITH
-- CTE 1: 当日放款聚合
agg AS (
    SELECT
        src.product_code,
        SUM(src.loan_amount)             AS td_sum_loan_amt,
        COUNT(src.loan_id)               AS td_cnt_loan
    FROM dwd.dwd_loan_detail src
    WHERE src.dt = '${hivevar:dt}'
    GROUP BY src.product_code
),
-- CTE 2: 昨日放款金额（用于环比计算）
agg_prev AS (
    SELECT
        src.product_code,
        SUM(src.loan_amount)             AS yd_sum_loan_amt
    FROM dwd.dwd_loan_detail src
    WHERE src.dt = DATE_ADD('${hivevar:dt}', -1)
    GROUP BY src.product_code
)

INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily
PARTITION (dt)
SELECT
    -- ===== 维度字段 =====
    a.product_code,                                              -- 产品编码
    dim_prod.product_name,                                       -- 产品名称

    -- ===== 指标字段 =====
    COALESCE(a.td_sum_loan_amt, 0)       AS td_sum_loan_amt,    -- 当日放款总金额
    COALESCE(a.td_cnt_loan, 0)           AS td_cnt_loan,        -- 当日放款笔数
    COALESCE(a.td_sum_loan_amt, 0)
        - COALESCE(ap.yd_sum_loan_amt, 0)
                                         AS td_diff_loan_amt,   -- 日环比差值

    -- ===== 分区字段 =====
    '${hivevar:dt}'                      AS dt

FROM agg a
-- 关联维度: 产品名称
LEFT JOIN dim.dim_product dim_prod
    ON a.product_code = dim_prod.product_code
-- 关联: 昨日数据（环比）
LEFT JOIN agg_prev ap
    ON a.product_code = ap.product_code
;
```

### 初始化脚本示例

当使用 `--mode=init` 时，还会生成以下初始化脚本：

**生成脚本：** `dm/dmm_sac_loan_prod_daily_init.sql`

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_init.sql
-- 功能:    贷款产品日维度指标宽表 - 历史数据初始化
-- 目标表:  dm.dmm_sac_loan_prod_daily
-- 源表:    dwd.dwd_loan_detail, dim.dim_product
-- 粒度:    一行 = 一天 × 一产品
-- 作者:    auto-generated
-- 创建日期: 2026-01-31
-- ============================================================
--
-- 使用场景: 新表上线时一次性回刷历史数据
--
-- 执行方式:
--   方式 1 (指定日期范围):
--     hive -hivevar start_dt=2024-01-01 -hivevar end_dt=2024-12-31 \
--          -f dmm_sac_loan_prod_daily_init.sql
--
--   方式 2 (使用 Shell 计算最近 N 天):
--     start_dt=$(date -d "30 days ago" +%Y-%m-%d)
--     end_dt=$(date -d "yesterday" +%Y-%m-%d)
--     hive -hivevar start_dt=$start_dt -hivevar end_dt=$end_dt \
--          -f dmm_sac_loan_prod_daily_init.sql
--
-- 注意事项:
--   1. 仅在新表上线或需要全量修复时执行
--   2. 大数据量时建议分批执行（如按月回刷）
--   3. 执行前确认目标表分区可覆盖
--   4. 日常增量调度使用 dmm_sac_loan_prod_daily_etl.sql
--
-- ============================================================

-- === 动态分区配置（初始化脚本必需）===
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.parallel=true;

-- === ETL 主逻辑 ===
WITH
-- CTE 1: 时间范围内放款聚合（按日期分组）
agg AS (
    SELECT
        src.dt,                                  -- 新增：分区字段
        src.product_code,
        SUM(src.loan_amount)             AS td_sum_loan_amt,
        COUNT(src.loan_id)               AS td_cnt_loan
    FROM dwd.dwd_loan_detail src
    WHERE src.dt BETWEEN '${hivevar:start_dt}' AND '${hivevar:end_dt}'  -- 时间范围过滤
    GROUP BY src.dt, src.product_code          -- 新增：dt 分组
),
-- CTE 2: 计算每天的昨日数据（用于环比）
agg_with_prev AS (
    SELECT
        dt,
        product_code,
        td_sum_loan_amt,
        td_cnt_loan,
        LAG(td_sum_loan_amt, 1) OVER (PARTITION BY product_code ORDER BY dt) AS yd_sum_loan_amt
    FROM agg
)

INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily
PARTITION (dt)  -- 动态分区
SELECT
    -- ===== 维度字段 =====
    a.product_code,                                              -- 产品编码
    dim_prod.product_name,                                       -- 产品名称

    -- ===== 指标字段 =====
    COALESCE(a.td_sum_loan_amt, 0)       AS td_sum_loan_amt,    -- 当日放款总金额
    COALESCE(a.td_cnt_loan, 0)           AS td_cnt_loan,        -- 当日放款笔数
    COALESCE(a.td_sum_loan_amt, 0)
        - COALESCE(a.yd_sum_loan_amt, 0)
                                         AS td_diff_loan_amt,   -- 日环比差值

    -- ===== 分区字段 =====
    a.dt                                 AS dt                   -- 动态分区字段

FROM agg_with_prev a
-- 关联维度: 产品名称
LEFT JOIN dim.dim_product dim_prod
    ON a.product_code = dim_prod.product_code
;
```

**关键差异对比**:

| 元素 | 增量脚本 | 初始化脚本 |
|------|---------|-----------|
| **分区写入** | `PARTITION (dt)` 静态 | `PARTITION (dt)` 动态 |
| **时间过滤** | `WHERE dt = '${dt}'` | `WHERE dt BETWEEN '${start_dt}' AND '${end_dt}'` |
| **GROUP BY** | `GROUP BY product_code` | `GROUP BY dt, product_code` |
| **环比计算** | LEFT JOIN 昨日聚合 | 使用 LAG 窗口函数（性能更优） |
| **SELECT 分区** | `'${hivevar:dt}' AS dt` 静态 | `a.dt AS dt` 动态来源 |
| **动态分区配置** | 不需要 | 必须开启 |

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
   来源: dm.dmm_sac_loan_prod_daily

2. 当日放款笔数 (td_cnt_loan)
   口径: 当日放款订单去重计数
   来源: dm.dmm_sac_loan_prod_daily

请确认哪些需要注册为公共指标？
(A) 全部注册
(B) 仅注册第 [N] 项（逗号分隔）
(C) 全部不注册
```

### 5.3 执行入库

用户确认后，调用 `register_indicator` 批量写入：

```
调用: register_indicator({
    "indicators": [
        {
            "indicator_name": "当日放款金额",
            "target_column": "td_sum_loan_amt",
            "source_table": "dm.dmm_sac_loan_prod_daily",
            "logic_desc": "当日所有放款订单金额之和，单位：元"
        },
        ...
    ],
    "created_by": "zhangsan"
})
```

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
INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily PARTITION (dt)
SELECT ...
FROM dwd.dwd_loan_detail src
LEFT JOIN dim.dim_product dim_prod ON ...
LEFT JOIN agg_prev ap ON ...
```

**提取结果**:

```json
{
  "target_table": "dm.dmm_sac_loan_prod_daily",
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
    "target_table": "dm.dmm_sac_loan_prod_daily",
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

### 6.4 在脚本头部添加血缘注释

在生成的 ETL 脚本头部注释中，添加血缘信息摘要：

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_etl.sql
-- 功能:    加工贷款产品日维度指标宽表
-- 目标表:  dm.dmm_sac_loan_prod_daily
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
    "table_name": "dm.dmm_sac_loan_prod_daily",
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
- 常量字段（如 `'${dt}' AS dt`）
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
