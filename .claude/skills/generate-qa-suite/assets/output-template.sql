-- ============================================================
-- 模板说明: generate-qa-suite 标准输出格式
-- 此文件为 Hive 引擎模板，Impala/Doris 需适配语法差异
-- ============================================================


-- ============================================================
-- QA Suite: {schema}.{target_table}
-- ETL 脚本: {etl_script_path}
-- 源表:     {source_table_1}, {source_table_2}
-- 逻辑主键: {pk_col_1}, {pk_col_2}
-- 引擎:     {Hive|Impala|Doris}
-- 生成时间:  {YYYY-MM-DD HH:MM:SS}
-- ============================================================


-- ==================== Part 1: 冒烟测试 ====================
-- 用途: ETL 执行后立即运行，快速判断是否正常
-- 耗时: < 1 分钟
-- ============================================================

-- [S-01] 目标表行数 (FATAL: 必须 > 0)
SELECT 'S-01: 目标表行数' AS test_id,
       COUNT(*)            AS result,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM {target_table}
WHERE dt = '${hivevar:dt}';

-- [S-02] 源表 vs 目标表行数 (WARN: 比例异常)
SELECT 'S-02: 行数对比' AS test_id,
       src_cnt, tgt_cnt,
       ROUND(tgt_cnt / NULLIF(src_cnt, 0), 4) AS ratio,
       CASE
           WHEN tgt_cnt = 0 THEN 'FATAL'
           WHEN tgt_cnt > src_cnt THEN 'WARN: 目标 > 源表'
           ELSE 'PASS'
       END AS status
FROM (
    SELECT
        (SELECT COUNT(*) FROM {source_table} WHERE dt = '${hivevar:dt}') AS src_cnt,
        (SELECT COUNT(*) FROM {target_table} WHERE dt = '${hivevar:dt}') AS tgt_cnt
) t;

-- [S-03] 主键唯一性 (FATAL: 必须为 0)
SELECT 'S-03: 主键重复' AS test_id,
       COUNT(*)          AS dup_group_cnt,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM (
    SELECT {pk_col_1}, {pk_col_2}
    FROM {target_table}
    WHERE dt = '${hivevar:dt}'
    GROUP BY {pk_col_1}, {pk_col_2}
    HAVING COUNT(*) > 1
) dup;

-- [S-04] 关键字段非 NULL (ERROR)
SELECT 'S-04: NULL值' AS test_id,
       SUM(
           CASE WHEN {pk_col_1} IS NULL THEN 1 ELSE 0 END
         + CASE WHEN {pk_col_2} IS NULL THEN 1 ELSE 0 END
       ) AS null_cnt,
       CASE WHEN SUM(
           CASE WHEN {pk_col_1} IS NULL THEN 1 ELSE 0 END
         + CASE WHEN {pk_col_2} IS NULL THEN 1 ELSE 0 END
       ) = 0 THEN 'PASS' ELSE 'ERROR' END AS status
FROM {target_table}
WHERE dt = '${hivevar:dt}';

-- [S-05] 数据样本 (INFO: 人工抽查)
SELECT *
FROM {target_table}
WHERE dt = '${hivevar:dt}'
LIMIT 20;


-- ==================== Part 2: DQC 规则 ====================
-- 用途: 系统化质量检查，可接入调度平台作为卡点
-- 结果: 统一输出 rule_id / target / result
-- ============================================================

-- [DQC-U01] 主键唯一
SELECT 'DQC-U01' AS rule_id, '{pk_cols}' AS target,
       COUNT(*) AS dup_group_cnt,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT {pk_col_1}, {pk_col_2}, COUNT(*) AS cnt
    FROM {target_table} WHERE dt = '${hivevar:dt}'
    GROUP BY {pk_col_1}, {pk_col_2}
    HAVING COUNT(*) > 1
) t;

-- [DQC-C02] {dim_code_col} 非 NULL
SELECT 'DQC-C02' AS rule_id, '{dim_code_col}' AS target,
       SUM(CASE WHEN {dim_code_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt,
       CASE WHEN SUM(CASE WHEN {dim_code_col} IS NULL THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table} WHERE dt = '${hivevar:dt}';

-- [DQC-V01] {amt_col} 非负
SELECT 'DQC-V01' AS rule_id, '{amt_col}' AS target,
       SUM(CASE WHEN {amt_col} < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN {amt_col} < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table} WHERE dt = '${hivevar:dt}';

-- [DQC-V02] {rat_col} 范围 [0,1]
SELECT 'DQC-V02' AS rule_id, '{rat_col}' AS target,
       SUM(CASE WHEN {rat_col} < 0 OR {rat_col} > 1 THEN 1 ELSE 0 END) AS out_cnt,
       CASE WHEN SUM(CASE WHEN {rat_col} < 0 OR {rat_col} > 1 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table} WHERE dt = '${hivevar:dt}';

-- [DQC-V03] {bool_col} 枚举 {0,1}
SELECT 'DQC-V03' AS rule_id, '{bool_col}' AS target,
       SUM(CASE WHEN {bool_col} NOT IN (0, 1) THEN 1 ELSE 0 END) AS invalid_cnt,
       CASE WHEN SUM(CASE WHEN {bool_col} NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table} WHERE dt = '${hivevar:dt}';

-- [DQC-V04] {cnt_col} 非负
SELECT 'DQC-V04' AS rule_id, '{cnt_col}' AS target,
       SUM(CASE WHEN {cnt_col} < 0 THEN 1 ELSE 0 END) AS negative_cnt,
       CASE WHEN SUM(CASE WHEN {cnt_col} < 0 THEN 1 ELSE 0 END) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM {target_table} WHERE dt = '${hivevar:dt}';

-- [DQC-CS02] 跨层金额一致
SELECT 'DQC-CS02' AS rule_id, '{amt_col} 汇总一致' AS target,
       ABS(tgt_sum - src_sum) AS diff,
       CASE WHEN ABS(tgt_sum - src_sum) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT
        (SELECT SUM({tgt_amt_col}) FROM {target_table} WHERE dt = '${hivevar:dt}') AS tgt_sum,
        (SELECT SUM({src_amt_col}) FROM {source_table} WHERE dt = '${hivevar:dt}') AS src_sum
) t;

-- [DQC-VOL01] 行数波动 (T vs T-1)
SELECT 'DQC-VOL01' AS rule_id, '行数波动' AS target,
       today_cnt, yesterday_cnt,
       ROUND(ABS(today_cnt - yesterday_cnt) / NULLIF(yesterday_cnt, 0), 4) AS volatility,
       CASE
           WHEN yesterday_cnt = 0 AND today_cnt > 0 THEN 'PASS'
           WHEN yesterday_cnt = 0 AND today_cnt = 0 THEN 'WARN: 连续为空'
           WHEN ABS(today_cnt - yesterday_cnt) / yesterday_cnt > 0.5 THEN 'WARN: >50%'
           ELSE 'PASS'
       END AS result
FROM (
    SELECT
        (SELECT COUNT(*) FROM {target_table} WHERE dt = '${hivevar:dt}') AS today_cnt,
        (SELECT COUNT(*) FROM {target_table} WHERE dt = DATE_ADD('${hivevar:dt}', -1)) AS yesterday_cnt
) t;


-- ==================== Part 3: Doris 性能分析 (仅 Doris) ====================
-- 用途: 查询上线前检查执行计划，识别瓶颈
-- 注意: 以下仅在 Doris 引擎时输出
-- ============================================================

-- [PERF-01] 执行计划
-- EXPLAIN
-- {etl_select_query};

-- [PERF-02] 详细执行计划
-- EXPLAIN VERBOSE
-- {etl_select_query};

-- 检查清单:
-- □ OlapScanNode.partitions = 1/N (分区裁剪生效)
-- □ PREAGGREGATION = ON (预聚合生效)
-- □ rollup 命中预期物化视图
-- □ 小表 JOIN 类型 = BROADCAST
-- □ Predicates 包含分区过滤条件
-- □ 两阶段聚合 (update + merge)
