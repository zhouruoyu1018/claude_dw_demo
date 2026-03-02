# SQL 构建指南 (SQL Construction Guide)

> 本文件是 `generate-etl-sql` Step 3 的详细构建规则。
> SKILL.md 中仅保留 Step 3 骨架，具体模板和规范引用本文件。

---

## 1. 脚本结构

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

---

## 2. 脚本头部注释

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

---

## 3. SET 参数配置

### Hive (Tez)

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

### Impala

```sql
-- === Impala 执行参数 ===
SET MEM_LIMIT=8g;
SET REQUEST_POOL='etl_pool';
-- SET COMPRESSION_CODEC='snappy';
```

### Doris

Doris 无需 SET 参数，通过 SQL Hint 或 Session Variable 控制：
```sql
-- SET enable_vectorized_engine = true;
-- SET parallel_fragment_exec_instance_num = 8;
```

---

## 4. INSERT OVERWRITE 模板

### Hive/Impala — 分区覆写

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

### Doris — INSERT INTO（Unique Model Upsert）

```sql
INSERT INTO {target_db}.{target_table}
SELECT
    {col_list}
FROM {source}
WHERE partition_key = '${partition_key}'
GROUP BY {group_cols}
;
```

---

## 5. SELECT 字段列表规范

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

---

## 6. JOIN 规范

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

---

## 7. WHERE 条件规范

```sql
WHERE src.stat_date = '${stat_date}'         -- 分区过滤（必须）
  AND src.is_deleted = 0             -- 逻辑删除过滤
  AND src.loan_status IN (...)       -- 业务条件
```

- 分区过滤条件**必须写在第一行**
- 使用 `${stat_date}` 参数化日期，由调度系统注入
- Hive 中使用 `${hivevar:stat_date}`，Impala 中使用 `${var:stat_date}`

---

## 8. GROUP BY 规范

```sql
GROUP BY
    src.product_code,
    src.product_name
```

- 与 SELECT 中的非聚合字段严格一致
- 不使用列序号（`GROUP BY 1, 2`），使用完整字段名
- 每个字段独占一行
