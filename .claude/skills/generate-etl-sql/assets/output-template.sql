-- ============================================================
-- 模板说明: generate-etl-sql 标准输出格式
-- 此文件为 Hive 引擎模板，Impala/Doris 需适配语法差异
-- ============================================================


-- ============================================================
-- 脚本:    {schema}/{table_name}_etl.sql
-- 功能:    {功能描述}
-- 目标表:  {schema}.{table_name}
-- 源表:    {source_table_1}, {source_table_2}, ...
-- 粒度:    {一行 = 一天 × 一产品}
-- 调度:    {每日/每周/每月} {T+1}
-- 依赖:    {上游表列表}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 初始创建
-- ============================================================


-- ========================
-- [1] 执行参数
-- ========================

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
-- SET hive.auto.convert.join=true;          -- 按需启用: 大表关联
-- SET hive.optimize.skewjoin=true;          -- 按需启用: 数据倾斜


-- ========================
-- [2] ETL 主逻辑
-- ========================

WITH
-- CTE 1: {描述}
{cte_name_1} AS (
    SELECT
        {col_1},
        {col_2},
        {agg_expression}                 AS {metric_col}
    FROM {source_schema}.{source_table} src
    WHERE src.dt = '${hivevar:dt}'
    GROUP BY
        {col_1},
        {col_2}
),

-- CTE 2: {描述}（如需要）
{cte_name_2} AS (
    SELECT
        {col_1},
        {window_expression}              AS {window_col}
    FROM {cte_name_1}
)

INSERT OVERWRITE TABLE {target_schema}.{target_table}
PARTITION (dt)
SELECT
    -- ===== 维度字段 =====
    {cte_alias}.{dim_col_1},                                    -- {中文注释}
    dim_{entity}.{dim_name_col},                                -- {中文注释}

    -- ===== 布尔字段 =====
    CASE
        WHEN {condition} THEN 1
        ELSE 0
    END                                  AS {bool_col},         -- {中文注释}，0-否 1-是

    -- ===== 指标字段 =====
    COALESCE({cte_alias}.{metric_col_1}, 0)
                                         AS {metric_col_1},     -- {中文注释}
    COALESCE({cte_alias}.{metric_col_2}, 0)
                                         AS {metric_col_2},     -- {中文注释}

    -- ===== 分区字段（末尾） =====
    '${hivevar:dt}'                      AS dt

FROM {cte_name_1} {cte_alias}
-- 关联维度: {目的}
LEFT JOIN {dim_schema}.{dim_table} dim_{entity}
    ON {cte_alias}.{join_key} = dim_{entity}.{join_key}
-- 关联: {目的}（如需要）
LEFT JOIN {cte_name_2} {cte_alias_2}
    ON {cte_alias}.{join_key} = {cte_alias_2}.{join_key}
;


-- ========================
-- [3] 数据质量校验（可选）
-- ========================

-- 3.1 行数校验
SELECT '目标行数' AS check_item, COUNT(*) AS cnt
FROM {target_schema}.{target_table}
WHERE dt = '${hivevar:dt}'
UNION ALL
SELECT '源表行数', COUNT(*)
FROM {source_schema}.{source_table}
WHERE dt = '${hivevar:dt}';

-- 3.2 主键唯一性
SELECT '主键重复数' AS check_item, COUNT(*) AS cnt
FROM (
    SELECT {pk_col_1}, {pk_col_2}, COUNT(*) AS dup_cnt
    FROM {target_schema}.{target_table}
    WHERE dt = '${hivevar:dt}'
    GROUP BY {pk_col_1}, {pk_col_2}
    HAVING COUNT(*) > 1
) t;

-- 3.3 关键字段 NULL 值
SELECT '关键字段NULL数' AS check_item, COUNT(*) AS cnt
FROM {target_schema}.{target_table}
WHERE dt = '${hivevar:dt}'
  AND ({key_col_1} IS NULL OR {key_col_2} IS NULL);
