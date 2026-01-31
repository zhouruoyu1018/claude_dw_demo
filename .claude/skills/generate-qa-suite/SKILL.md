---
name: generate-qa-suite
description: 测试与 DQC 生成。根据 ETL SQL 和业务需求，自动生成冒烟测试 SQL、数据质量校验（DQC）规则和 Doris 性能分析请求。使用场景：(1) ETL 脚本开发完成后需要验证正确性 (2) 上线前数据质量卡点检查 (3) 日常调度后自动化质量巡检 (4) Doris 查询性能分析与优化
---

# 测试与 DQC 生成 (Generate QA Suite)

根据 ETL SQL 代码和业务需求，自动生成完整的质量验证套件。

## 定位

**质检员** — 上游 Skill 完成"施工"（ETL SQL），本 Skill 负责"验收"（测试 + 质量检查 + 性能分析）。

## 输入输出

### 输入

| 来源 | 内容 | 必需 |
|------|------|------|
| `generate-etl-sql` | ETL SQL 脚本（含源表、目标表、加工逻辑） | 是 |
| `generate-standard-ddl` | 目标表 DDL（含逻辑主键、字段类型、COMMENT） | 是 |
| 业务需求 / `dw-requirement-triage` | 业务规则、指标口径、预期范围 | 是 |
| 引擎类型 | Hive / Impala / Doris | 是 |

### 输出

完整的 QA 套件，包含三部分：

```
[Part 1] 冒烟测试 SQL     — ETL 执行后立即跑，快速判断是否正常
[Part 2] DQC 规则 SQL      — 系统化质量检查，可接入调度平台
[Part 3] 性能分析（Doris）  — EXPLAIN 分析 + 优化建议
```

---

## 核心工作流

```
ETL SQL + DDL + 业务需求
    ↓
┌──────────────────────────────┐
│ Step 1: 解析 ETL 上下文      │
│ 提取源表/目标表/主键/字段   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 2: 生成冒烟测试         │
│ 行数 / 主键 / 空值 / 样本   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 3: 生成 DQC 规则        │
│ 按字段类型 + 业务语义匹配   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 性能分析（Doris）    │
│ EXPLAIN + 瓶颈识别          │
└──────────────────────────────┘
    ↓
输出完整 QA 套件
```

---

## Step 1: 解析 ETL 上下文

从 ETL SQL 脚本和目标表 DDL 中自动提取：

| 提取项 | 来源 | 示例 |
|--------|------|------|
| 目标表 | `INSERT OVERWRITE TABLE ...` | `dm.dmm_sac_loan_prod_daily` |
| 源表列表 | `FROM` / `JOIN` 子句 | `dwd.dwd_loan_detail`, `dim.dim_product` |
| 逻辑主键 | DDL TBLPROPERTIES `logical_primary_key` | `product_code, dt` |
| 分区字段 | DDL `PARTITIONED BY` | `dt` |
| 维度字段 | DDL 注释分组"维度字段" | `product_code`, `product_name` |
| 指标字段 | DDL 注释分组"指标字段" | `td_sum_loan_amt`, `td_cnt_loan` |
| 布尔字段 | `is_` / `has_` 前缀 | `is_first_overdue` |
| 金额字段 | 类型 `DECIMAL(18,2)` + COMMENT 含"金额" | `td_sum_loan_amt` |
| 比率字段 | 类型 `DECIMAL(10,4)` + COMMENT 含"率" | `rat_overdue_m1` |
| 计数字段 | 类型 `BIGINT` + COMMENT 含"笔数/件数" | `td_cnt_loan` |
| 引擎类型 | 脚本 SET 参数或用户指定 | Hive / Impala / Doris |

---

## Step 2: 生成冒烟测试

冒烟测试用于 ETL 执行后**快速验证**，判断本次运行是否正常。应在 1 分钟内完成。

### 2.1 测试项清单

| 编号 | 测试项 | 目的 | 严重级别 |
|------|--------|------|---------|
| S-01 | 目标表行数 > 0 | 确认数据已写入 | **FATAL** |
| S-02 | 源表行数 vs 目标表行数 | 聚合比例是否合理 | WARN |
| S-03 | 主键唯一性 | 无重复数据 | **FATAL** |
| S-04 | 关键字段非 NULL | 维度和核心指标不为空 | ERROR |
| S-05 | 数据样本抽查 | 人工可视化检查 | INFO |
| S-06 | 分区写入确认 | 目标分区存在 | **FATAL** |

### 2.2 SQL 生成模板

```sql
-- ============================================================
-- 冒烟测试: {target_table}
-- ETL 日期: ${dt}
-- 生成时间: {YYYY-MM-DD HH:MM:SS}
-- ============================================================

-- [S-01] 目标表行数 (FATAL: 必须 > 0)
SELECT 'S-01: 目标表行数' AS test_id,
       COUNT(*)           AS result,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM {target_table}
WHERE dt = '${dt}';

-- [S-02] 源表行数对比 (WARN: 比例异常时预警)
SELECT 'S-02: 行数对比' AS test_id,
       src_cnt, tgt_cnt,
       ROUND(tgt_cnt / NULLIF(src_cnt, 0), 4) AS ratio,
       CASE
           WHEN tgt_cnt = 0 THEN 'FATAL'
           WHEN tgt_cnt > src_cnt THEN 'WARN: 目标 > 源表，检查是否 JOIN 膨胀'
           ELSE 'PASS'
       END AS status
FROM (
    SELECT
        (SELECT COUNT(*) FROM {source_table} WHERE dt = '${dt}') AS src_cnt,
        (SELECT COUNT(*) FROM {target_table} WHERE dt = '${dt}') AS tgt_cnt
) t;

-- [S-03] 主键唯一性 (FATAL: 必须为 0)
SELECT 'S-03: 主键重复' AS test_id,
       COUNT(*)          AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM (
    SELECT {pk_cols}
    FROM {target_table}
    WHERE dt = '${dt}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) dup;

-- [S-04] 关键字段非 NULL (ERROR: 应为 0)
SELECT 'S-04: NULL值检查' AS test_id,
       {null_check_expression}   AS result,
       CASE WHEN {null_check_expression} = 0 THEN 'PASS' ELSE 'ERROR' END AS status
FROM {target_table}
WHERE dt = '${dt}';

-- [S-05] 数据样本 (INFO: 人工检查)
SELECT *
FROM {target_table}
WHERE dt = '${dt}'
LIMIT 20;

-- [S-06] 分区确认 (FATAL)
SHOW PARTITIONS {target_table};
-- 或 Hive:
-- SELECT DISTINCT dt FROM {target_table} WHERE dt = '${dt}';
```

### 2.3 NULL 检查表达式生成

根据字段重要性自动选择需要检查的字段：

```sql
-- 维度字段 + 核心指标全部检查
SUM(
    CASE WHEN {dim_col_1} IS NULL THEN 1 ELSE 0 END
  + CASE WHEN {dim_col_2} IS NULL THEN 1 ELSE 0 END
  + CASE WHEN {metric_col_1} IS NULL THEN 1 ELSE 0 END
)
```

优先级：
- **必检**: 逻辑主键字段、分区字段
- **应检**: 维度字段（_code, _id）、核心指标字段
- **可选**: 名称字段（_name）、辅助指标

---

## Step 3: 生成 DQC 规则

DQC 规则是系统化的质量检查，可接入调度平台（如 Airflow、DolphinScheduler）作为下游卡点。

### 3.1 规则自动匹配

根据字段类型和命名模式，自动匹配适用的 DQC 规则：

| 字段特征 | 自动匹配的规则 |
|---------|---------------|
| 逻辑主键 (`logical_primary_key`) | 唯一性检查 |
| 金额字段 (`DECIMAL` + `_amt`) | 非负检查、合理范围检查 |
| 比率字段 (`DECIMAL` + `_rat`/`_rate`) | [0, 1] 范围检查 |
| 计数字段 (`BIGINT` + `_cnt`) | 非负检查 |
| 布尔字段 (`TINYINT` + `is_`/`has_`) | {0, 1} 枚举检查 |
| 日期字段 (`STRING` + `_date`) | 格式检查（YYYY-MM-DD） |
| 维度编码 (`STRING` + `_code`) | 非空检查、引用完整性 |
| 所有指标 | 波动率检查（T vs T-1） |
| 全表 | 行数非零、行数波动 |

### 3.2 规则分类与模板

#### 3.2.1 完整性规则 (Completeness)

```sql
-- [DQC-C01] 表非空
SELECT 'DQC-C01: 表非空' AS rule_id,
       '{target_table}' AS target,
       COUNT(*) AS actual,
       'COUNT > 0' AS expected,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-C02] 关键字段非 NULL
SELECT 'DQC-C02: {col} 非NULL' AS rule_id,
       '{target_table}.{col}' AS target,
       SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_cnt,
       '0' AS expected,
       CASE WHEN SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-C03] 非空字符串（排除空串）
SELECT 'DQC-C03: {col} 非空串' AS rule_id,
       SUM(CASE WHEN {col} IS NULL OR TRIM({col}) = '' THEN 1 ELSE 0 END) AS empty_cnt,
       CASE WHEN SUM(CASE WHEN {col} IS NULL OR TRIM({col}) = '' THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';
```

#### 3.2.2 唯一性规则 (Uniqueness)

```sql
-- [DQC-U01] 主键唯一
SELECT 'DQC-U01: 主键唯一' AS rule_id,
       '{pk_cols}' AS target,
       COUNT(*) AS dup_group_cnt,
       '0' AS expected,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT {pk_cols}, COUNT(*) AS cnt
    FROM {target_table}
    WHERE dt = '${dt}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) t;
```

#### 3.2.3 有效性规则 (Validity)

```sql
-- [DQC-V01] 金额非负
SELECT 'DQC-V01: {amt_col} >= 0' AS rule_id,
       SUM(CASE WHEN {amt_col} < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN {amt_col} < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-V02] 比率范围 [0, 1]
SELECT 'DQC-V02: {rat_col} in [0,1]' AS rule_id,
       SUM(CASE WHEN {rat_col} < 0 OR {rat_col} > 1 THEN 1 ELSE 0 END) AS out_of_range_cnt,
       CASE WHEN SUM(CASE WHEN {rat_col} < 0 OR {rat_col} > 1 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-V03] 布尔值枚举 {0, 1}
SELECT 'DQC-V03: {bool_col} in (0,1)' AS rule_id,
       SUM(CASE WHEN {bool_col} NOT IN (0, 1) THEN 1 ELSE 0 END) AS invalid_cnt,
       CASE WHEN SUM(CASE WHEN {bool_col} NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-V04] 计数非负
SELECT 'DQC-V04: {cnt_col} >= 0' AS rule_id,
       SUM(CASE WHEN {cnt_col} < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN {cnt_col} < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';

-- [DQC-V05] 日期格式 (YYYY-MM-DD)
SELECT 'DQC-V05: {date_col} 格式' AS rule_id,
       SUM(CASE WHEN {date_col} NOT RLIKE '^\\d{4}-\\d{2}-\\d{2}$' THEN 1 ELSE 0 END) AS bad_fmt_cnt,
       CASE WHEN SUM(CASE WHEN {date_col} NOT RLIKE '^\\d{4}-\\d{2}-\\d{2}$' THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table}
WHERE dt = '${dt}';
```

#### 3.2.4 一致性规则 (Consistency)

```sql
-- [DQC-CS01] 引用完整性（维度编码在维表中存在）
SELECT 'DQC-CS01: {code_col} 引用完整' AS rule_id,
       COUNT(*) AS orphan_cnt,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM {target_table} t
LEFT JOIN {dim_table} d ON t.{code_col} = d.{code_col}
WHERE t.dt = '${dt}'
  AND d.{code_col} IS NULL
  AND t.{code_col} IS NOT NULL;

-- [DQC-CS02] 跨层数据一致（汇总层 vs 明细层）
SELECT 'DQC-CS02: 金额汇总一致' AS rule_id,
       ABS(tgt_sum - src_sum) AS diff,
       CASE WHEN ABS(tgt_sum - src_sum) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT
        (SELECT SUM({amt_col}) FROM {target_table} WHERE dt = '${dt}') AS tgt_sum,
        (SELECT SUM({src_amt_col}) FROM {source_table} WHERE dt = '${dt}') AS src_sum
) t;
```

#### 3.2.5 波动率规则 (Volatility)

```sql
-- [DQC-VOL01] 行数波动（T vs T-1）
SELECT 'DQC-VOL01: 行数波动' AS rule_id,
       today_cnt, yesterday_cnt,
       ROUND(ABS(today_cnt - yesterday_cnt) / NULLIF(yesterday_cnt, 0), 4) AS volatility,
       CASE
           WHEN yesterday_cnt = 0 AND today_cnt > 0 THEN 'PASS: 首次写入'
           WHEN yesterday_cnt = 0 AND today_cnt = 0 THEN 'WARN: 连续为空'
           WHEN ABS(today_cnt - yesterday_cnt) / yesterday_cnt > 0.5 THEN 'WARN: 波动 >50%'
           ELSE 'PASS'
       END AS result
FROM (
    SELECT
        (SELECT COUNT(*) FROM {target_table} WHERE dt = '${dt}') AS today_cnt,
        (SELECT COUNT(*) FROM {target_table} WHERE dt = DATE_ADD('${dt}', -1)) AS yesterday_cnt
) t;

-- [DQC-VOL02] 指标波动（T vs T-1）
SELECT 'DQC-VOL02: {metric_col} 波动' AS rule_id,
       today_val, yesterday_val,
       ROUND(ABS(today_val - yesterday_val) / NULLIF(yesterday_val, 0), 4) AS volatility,
       CASE
           WHEN ABS(today_val - yesterday_val) / NULLIF(yesterday_val, 0) > 1.0 THEN 'WARN: 波动 >100%'
           ELSE 'PASS'
       END AS result
FROM (
    SELECT
        (SELECT SUM({metric_col}) FROM {target_table} WHERE dt = '${dt}') AS today_val,
        (SELECT SUM({metric_col}) FROM {target_table} WHERE dt = DATE_ADD('${dt}', -1)) AS yesterday_val
) t;
```

### 3.3 规则严重级别

| 级别 | 含义 | 调度动作 |
|------|------|---------|
| **FATAL** | 数据不可用 | 阻断下游任务，立即告警 |
| **ERROR** | 数据质量严重问题 | 告警，人工确认后放行 |
| **WARN** | 数据波动异常 | 告警，不阻断 |
| **INFO** | 信息性检查 | 仅记录 |

### 3.4 规则输出汇总表

所有规则结果写入统一汇总：

```sql
-- DQC 结果汇总
SELECT
    '${dt}'          AS check_date,
    rule_id,
    target,
    expected,
    actual,
    result,
    CURRENT_TIMESTAMP AS check_time
FROM (
    -- 各规则 UNION ALL
    {rule_1}
    UNION ALL
    {rule_2}
    UNION ALL
    ...
) all_rules
ORDER BY
    CASE result
        WHEN 'FATAL' THEN 1
        WHEN 'FAIL'  THEN 2
        WHEN 'ERROR' THEN 3
        WHEN 'WARN'  THEN 4
        ELSE 5
    END;
```

---

## Step 4: 性能分析（Doris 专项）

当目标引擎为 Doris 时，额外生成性能分析请求。

### 4.1 EXPLAIN 分析

```sql
-- [PERF-01] 查看执行计划
EXPLAIN
SELECT {etl_query_body};

-- [PERF-02] 查看详细执行计划（含统计信息）
EXPLAIN VERBOSE
SELECT {etl_query_body};

-- [PERF-03] 查看物理执行计划
EXPLAIN GRAPH
SELECT {etl_query_body};
```

### 4.2 EXPLAIN 关注点

生成 EXPLAIN 后，自动给出检查清单：

| 检查项 | 关注点 | 优化方向 |
|--------|--------|---------|
| `OlapScanNode` | 扫描行数是否过大 | 检查分区裁剪、物化视图 |
| `HASH JOIN` | 右表大小、Join 类型 | 小表 Broadcast vs Shuffle |
| `AGGREGATE` | 聚合方式 | 预聚合模型 vs 实时聚合 |
| `SORT` | 是否全局排序 | 避免不必要的 ORDER BY |
| `EXCHANGE` | 数据 Shuffle 量 | Colocate Join 消除 Shuffle |
| `Predicates` | 谓词是否下推到 Scan 层 | 确认分区/桶裁剪生效 |

### 4.3 Profile 分析（运行后）

```sql
-- 启用 Profile 收集
SET is_report_success = true;

-- 执行 ETL SQL
{etl_sql};

-- 查看最近 Profile
SHOW QUERY PROFILE '/';
-- 查看指定 Query Profile
SHOW QUERY PROFILE '/{query_id}';
```

### 4.4 Doris 特有检查

```sql
-- [PERF-D01] 表统计信息是否更新
SHOW TABLE STATS {target_table};

-- [PERF-D02] Tablet 分布是否均匀
SHOW TABLETS FROM {target_table};

-- [PERF-D03] Compaction 状态
SHOW TABLET {tablet_id};

-- [PERF-D04] 物化视图命中情况
EXPLAIN
SELECT {query_with_agg};
-- 检查输出中是否出现 rollup: {mv_name}
```

---

## 引擎适配

### 正则函数差异

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 正则匹配 | `col RLIKE 'pattern'` | `col REGEXP 'pattern'` | `col REGEXP 'pattern'` |
| 日期函数 | `DATE_ADD(dt, -1)` | `DAYS_SUB(dt, 1)` | `DATE_SUB(dt, INTERVAL 1 DAY)` |
| 分区查询 | `SHOW PARTITIONS t` | `SHOW PARTITIONS t` | `SHOW PARTITIONS FROM t` |
| EXPLAIN | `EXPLAIN` | `EXPLAIN` | `EXPLAIN` / `EXPLAIN VERBOSE` / `EXPLAIN GRAPH` |

### 冒烟测试引擎适配

- **Hive**: 使用 `${hivevar:dt}`，`RLIKE` 正则
- **Impala**: 使用 `${var:dt}`，`REGEXP` 正则
- **Doris**: 硬编码日期或应用层替换，`REGEXP` 正则，额外输出 EXPLAIN

---

## 完整示例

**目标表**: `dm.dmm_sac_loan_prod_daily`
**逻辑主键**: `product_code, dt`
**字段**: `product_code STRING`, `product_name STRING`, `td_sum_loan_amt DECIMAL(18,2)`, `td_cnt_loan BIGINT`, `is_first_loan TINYINT`, `rat_overdue_m1 DECIMAL(10,4)`

**自动生成的规则匹配：**

| 字段 | 特征 | 命中规则 |
|------|------|---------|
| `product_code, dt` | 逻辑主键 | DQC-U01 唯一性 |
| `product_code` | 维度编码 `_code` | DQC-C02 非NULL, DQC-CS01 引用完整 |
| `td_sum_loan_amt` | 金额 `DECIMAL` + `_amt` | DQC-V01 非负, DQC-VOL02 波动 |
| `td_cnt_loan` | 计数 `BIGINT` + `_cnt` | DQC-V04 非负 |
| `is_first_loan` | 布尔 `TINYINT` + `is_` | DQC-V03 枚举{0,1} |
| `rat_overdue_m1` | 比率 `DECIMAL(10,4)` + `rat_` | DQC-V02 范围[0,1] |
| 全表 | — | S-01 非空, S-02 行数对比, DQC-VOL01 行数波动 |

**生成输出（Hive）：**

```sql
-- ============================================================
-- QA Suite: dm.dmm_sac_loan_prod_daily
-- 生成时间: 2026-01-27
-- 引擎: Hive
-- ============================================================

-- ==================== Part 1: 冒烟测试 ====================

-- [S-01] 目标表行数
SELECT 'S-01' AS test_id, COUNT(*) AS result,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [S-02] 行数对比
SELECT 'S-02' AS test_id, src_cnt, tgt_cnt,
       ROUND(tgt_cnt / NULLIF(src_cnt, 0), 4) AS ratio
FROM (
    SELECT
        (SELECT COUNT(*) FROM dwd.dwd_loan_detail WHERE dt = '${hivevar:dt}') AS src_cnt,
        (SELECT COUNT(*) FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}') AS tgt_cnt
) t;

-- [S-03] 主键唯一性
SELECT 'S-03' AS test_id, COUNT(*) AS dup_cnt,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM (
    SELECT product_code FROM dm.dmm_sac_loan_prod_daily
    WHERE dt = '${hivevar:dt}'
    GROUP BY product_code HAVING COUNT(*) > 1
) dup;

-- [S-05] 数据样本
SELECT * FROM dm.dmm_sac_loan_prod_daily
WHERE dt = '${hivevar:dt}' LIMIT 20;

-- ==================== Part 2: DQC 规则 ====================

-- [DQC-C02] product_code 非 NULL
SELECT 'DQC-C02' AS rule_id, 'product_code' AS col,
       SUM(CASE WHEN product_code IS NULL THEN 1 ELSE 0 END) AS null_cnt,
       CASE WHEN SUM(CASE WHEN product_code IS NULL THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [DQC-U01] 主键唯一
SELECT 'DQC-U01' AS rule_id, COUNT(*) AS dup_group_cnt,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT product_code, COUNT(*) FROM dm.dmm_sac_loan_prod_daily
    WHERE dt = '${hivevar:dt}'
    GROUP BY product_code HAVING COUNT(*) > 1
) t;

-- [DQC-V01] td_sum_loan_amt 非负
SELECT 'DQC-V01' AS rule_id, 'td_sum_loan_amt' AS col,
       SUM(CASE WHEN td_sum_loan_amt < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN td_sum_loan_amt < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [DQC-V02] rat_overdue_m1 范围 [0,1]
SELECT 'DQC-V02' AS rule_id, 'rat_overdue_m1' AS col,
       SUM(CASE WHEN rat_overdue_m1 < 0 OR rat_overdue_m1 > 1 THEN 1 ELSE 0 END) AS out_cnt,
       CASE WHEN SUM(CASE WHEN rat_overdue_m1 < 0 OR rat_overdue_m1 > 1 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [DQC-V03] is_first_loan 枚举 {0,1}
SELECT 'DQC-V03' AS rule_id, 'is_first_loan' AS col,
       SUM(CASE WHEN is_first_loan NOT IN (0, 1) THEN 1 ELSE 0 END) AS invalid_cnt,
       CASE WHEN SUM(CASE WHEN is_first_loan NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [DQC-V04] td_cnt_loan 非负
SELECT 'DQC-V04' AS rule_id, 'td_cnt_loan' AS col,
       SUM(CASE WHEN td_cnt_loan < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN td_cnt_loan < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}';

-- [DQC-VOL01] 行数波动
SELECT 'DQC-VOL01' AS rule_id,
       today_cnt, yesterday_cnt,
       ROUND(ABS(today_cnt - yesterday_cnt) / NULLIF(yesterday_cnt, 0), 4) AS volatility,
       CASE
           WHEN yesterday_cnt = 0 AND today_cnt > 0 THEN 'PASS'
           WHEN ABS(today_cnt - yesterday_cnt) / NULLIF(yesterday_cnt, 0) > 0.5 THEN 'WARN'
           ELSE 'PASS'
       END AS result
FROM (
    SELECT
        (SELECT COUNT(*) FROM dm.dmm_sac_loan_prod_daily WHERE dt = '${hivevar:dt}') AS today_cnt,
        (SELECT COUNT(*) FROM dm.dmm_sac_loan_prod_daily WHERE dt = DATE_ADD('${hivevar:dt}', -1)) AS yesterday_cnt
) t;
```

---

## 交互式确认

遇到以下情况时，主动询问用户：

1. **波动阈值**: "行数/指标波动超过多少算异常？默认行数 50%、指标 100%，需要调整吗？"

2. **比率范围**: "字段 `rat_overdue_m1` 默认检查 [0, 1] 范围。该比率是否可能超过 1（如以百分比存储）？"

3. **引用完整性**: "`product_code` 应关联哪张维度表做引用完整性检查？"

4. **跨层一致性**: "是否需要校验目标表指标汇总值与源表明细汇总值一致？"

5. **性能要求**: "Doris 查询是否有延迟要求？（如 < 3 秒），以便设定 EXPLAIN 性能基线。"

---

## 与其他 Skill 的协作

```
需求文档
    ↓
dw-requirement-triage            ← 需求拆解
    ↓
search-hive-metadata             ← 元数据搜索 + 指标复用
    ↓
generate-standard-ddl            ← 目标表 DDL
    ↓
generate-etl-sql                 ← ETL SQL
    ↓
generate-qa-suite                ← 本 Skill: 测试 + DQC + 性能分析
    ↓
调度上线（QA Suite 嵌入调度 DAG）
```

## References

- [references/dqc-rules-catalog.md](references/dqc-rules-catalog.md) - DQC 规则完整目录与阈值配置
- [references/doris-explain-guide.md](references/doris-explain-guide.md) - Doris EXPLAIN 执行计划解读指南
