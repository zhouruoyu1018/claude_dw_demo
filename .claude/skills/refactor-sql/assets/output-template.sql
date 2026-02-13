-- ============================================================
-- 脚本:      {target_sql_file}
-- 来源:      {source_sql_file}
-- 重构范围:   {scope}
-- 目标引擎:   {source_engine} -> {target_engine}
-- 风险等级:   {SAFE/LOW/MEDIUM/HIGH}
-- 应用规则:   {F-*, C*, P*, M*}
-- 生成时间:   {YYYY-MM-DD HH:MM:SS}
-- ============================================================

-- ========================
-- [0] 重构摘要
-- ========================
-- 1) {rule_id}: {change_summary}
-- 2) {rule_id}: {change_summary}
-- 3) {rule_id}: {change_summary}


-- ========================
-- [1] 参数区
-- ========================
-- Hive:
--   ${hivevar:stat_date}
-- Impala:
--   ${var:stat_date}

-- {optional_set_statements}


-- ========================
-- [2] 重构后主逻辑
-- ========================
WITH
base_{topic} AS (
    SELECT
        {explicit_cols}
    FROM {source_table} src
    WHERE src.stat_date = '{date_param}'
),
agg_{topic} AS (
    SELECT
        {group_cols},
        COALESCE(SUM({metric_col}), 0) AS {metric_alias}
    FROM base_{topic}
    GROUP BY
        {group_cols}
)
INSERT OVERWRITE TABLE {target_table}
PARTITION (stat_date = '{date_param}')
SELECT
    {select_cols}
FROM agg_{topic} a
LEFT JOIN {dim_table} d
    ON a.{join_key} = d.{join_key}
;


-- ========================
-- [3] A/B 验证 SQL（只读）
-- ========================

-- A/B-01 行数对比
SELECT 'old' AS side, COUNT(*) AS cnt
FROM {old_result_source}
WHERE stat_date = '{date_param}'
UNION ALL
SELECT 'new' AS side, COUNT(*) AS cnt
FROM {new_result_source}
WHERE stat_date = '{date_param}';

-- A/B-02 主键唯一性对比
SELECT 'old' AS side, COUNT(*) AS dup_cnt
FROM (
    SELECT {pk_cols}, COUNT(*) AS c
    FROM {old_result_source}
    WHERE stat_date = '{date_param}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) t
UNION ALL
SELECT 'new' AS side, COUNT(*) AS dup_cnt
FROM (
    SELECT {pk_cols}, COUNT(*) AS c
    FROM {new_result_source}
    WHERE stat_date = '{date_param}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) t;

-- A/B-03 核心指标聚合对比
SELECT
    '{metric_name}' AS metric_name,
    old_sum,
    new_sum,
    (new_sum - old_sum) AS delta
FROM (
    SELECT
        (SELECT COALESCE(SUM({metric_col}), 0)
         FROM {old_result_source}
         WHERE stat_date = '{date_param}') AS old_sum,
        (SELECT COALESCE(SUM({metric_col}), 0)
         FROM {new_result_source}
         WHERE stat_date = '{date_param}') AS new_sum
) s;
