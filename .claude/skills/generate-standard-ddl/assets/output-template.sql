-- ============================================================
-- 模板说明: generate-standard-ddl 标准输出格式
-- 此文件为模板示例，实际生成时替换 {} 占位符
-- ============================================================


-- ================================================================
-- [CREATE TABLE 模板]
-- ================================================================

-- ============================================================
-- 表名:    {schema}.{table_name}
-- 功能:    {表功能描述}
-- 来源:    {上游表，如 dwd.dwd_loan_detail}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 初始创建
-- ============================================================

DROP TABLE IF EXISTS {schema}.{table_name};

CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    -- ===== 维度字段 =====
    {dim_id_col}          BIGINT          COMMENT '{实体}ID',
    {dim_code_col}        STRING          COMMENT '{实体}编码',
    {dim_name_col}        STRING          COMMENT '{实体}名称',
    {dim_date_col}        STRING          COMMENT '{实体}日期，格式YYYY-MM-DD',
    {dim_status_col}      STRING          COMMENT '{实体}状态，{枚举值说明}',

    -- ===== 布尔字段 =====
    {is_xxx}              TINYINT         COMMENT '是否{描述}，0-否 1-是',

    -- ===== 指标字段 =====
    {td_sum_xxx}          DECIMAL(18,2)   COMMENT '当日{描述}，单位：元',
    {td_cnt_xxx}          BIGINT          COMMENT '当日{描述}',
    {cur_mon_sum_xxx}     DECIMAL(18,2)   COMMENT '当月累计{描述}，单位：元',
    {his_max_xxx}         INT             COMMENT '历史最大{描述}',
    {rat_xxx}             DECIMAL(10,4)   COMMENT '{描述}={分子}/{分母}'
)
COMMENT '{业务含义}，{更新频率}[粒度:{col1},{col2},dt]'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'logical_primary_key' = '{col1},{col2},dt',
    'business_owner' = '{负责人}',
    'data_layer' = '{dm|da}'
);


-- ================================================================
-- [ALTER TABLE 模板]
-- 注意：分区表必须加 CASCADE，确保新字段应用到所有已存在分区
-- ================================================================

-- ============================================================
-- 表名:    {schema}.{table_name}
-- 变更:    新增 {N} 个字段 — {变更原因/需求来源}
-- 作者:    {author}
-- 变更日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 新增 {字段列表摘要}
-- ============================================================

ALTER TABLE {schema}.{table_name} ADD COLUMNS (
    {new_col_1}    {TYPE}    COMMENT '{注释}',
    {new_col_2}    {TYPE}    COMMENT '{注释}'
) CASCADE;
