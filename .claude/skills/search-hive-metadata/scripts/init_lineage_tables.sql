-- ============================================================
-- 血缘管理表初始化脚本 (PostgreSQL)
-- 功能: 创建 data_lineage 和 column_lineage 表，并插入测试数据
-- 使用: psql -h host -U user -d indicator_db -f init_lineage_tables.sql
-- ============================================================

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS column_lineage CASCADE;
DROP TABLE IF EXISTS data_lineage CASCADE;

-- ============================================================
-- 1. 表级血缘表
-- ============================================================
CREATE TABLE IF NOT EXISTS data_lineage (
    id SERIAL PRIMARY KEY,

    -- 目标表信息
    target_table VARCHAR(200) NOT NULL,
    target_schema VARCHAR(100),

    -- 源表信息
    source_table VARCHAR(200) NOT NULL,
    source_schema VARCHAR(100),

    -- 关系类型
    relation_type VARCHAR(50) DEFAULT 'ETL',
    join_type VARCHAR(50),

    -- ETL 信息
    etl_script_path VARCHAR(500),
    etl_logic_summary VARCHAR(1000),

    -- 元信息
    created_by VARCHAR(100) DEFAULT 'auto',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- 添加注释
COMMENT ON TABLE data_lineage IS '表级数据血缘关系';
COMMENT ON COLUMN data_lineage.target_table IS '目标表完整名，如 dm.dmm_sac_loan_prod_daily';
COMMENT ON COLUMN data_lineage.target_schema IS '目标表所属库';
COMMENT ON COLUMN data_lineage.source_table IS '源表完整名';
COMMENT ON COLUMN data_lineage.source_schema IS '源表所属库';
COMMENT ON COLUMN data_lineage.relation_type IS '关系类型: ETL/VIEW/MANUAL';
COMMENT ON COLUMN data_lineage.join_type IS 'JOIN 类型: FROM/INNER/LEFT/RIGHT/FULL/CROSS';
COMMENT ON COLUMN data_lineage.etl_script_path IS 'ETL 脚本路径';
COMMENT ON COLUMN data_lineage.etl_logic_summary IS 'ETL 逻辑摘要';
COMMENT ON COLUMN data_lineage.is_active IS '是否有效（表删除后置为 FALSE）';

-- 索引
CREATE INDEX idx_lineage_target ON data_lineage(target_table);
CREATE INDEX idx_lineage_source ON data_lineage(source_table);
CREATE INDEX idx_lineage_active ON data_lineage(is_active);

-- ============================================================
-- 2. 字段级血缘表
-- ============================================================
CREATE TABLE IF NOT EXISTS column_lineage (
    id SERIAL PRIMARY KEY,

    -- 目标字段
    target_table VARCHAR(200) NOT NULL,
    target_column VARCHAR(200) NOT NULL,

    -- 源字段
    source_table VARCHAR(200) NOT NULL,
    source_column VARCHAR(200) NOT NULL,

    -- 转换逻辑
    transform_type VARCHAR(50),
    transform_expr VARCHAR(1000),

    -- 关联的表级血缘
    table_lineage_id INT REFERENCES data_lineage(id) ON DELETE SET NULL,

    -- 元信息
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加注释
COMMENT ON TABLE column_lineage IS '字段级数据血缘关系';
COMMENT ON COLUMN column_lineage.target_table IS '目标表完整名';
COMMENT ON COLUMN column_lineage.target_column IS '目标字段名';
COMMENT ON COLUMN column_lineage.source_table IS '源表完整名';
COMMENT ON COLUMN column_lineage.source_column IS '源字段名';
COMMENT ON COLUMN column_lineage.transform_type IS '转换类型: DIRECT/SUM/COUNT/AVG/MAX/MIN/CASE/CUSTOM';
COMMENT ON COLUMN column_lineage.transform_expr IS '转换表达式，如 SUM(loan_amount)';
COMMENT ON COLUMN column_lineage.table_lineage_id IS '关联的表级血缘ID';

-- 索引
CREATE INDEX idx_col_lineage_target ON column_lineage(target_table, target_column);
CREATE INDEX idx_col_lineage_source ON column_lineage(source_table, source_column);

-- ============================================================
-- 3. 插入测试数据 - 表级血缘
-- ============================================================

-- 场景1: dm.dmm_sac_loan_prod_daily (贷款产品日报表)
--   依赖: dwd.dwd_loan_detail, dim.dim_product
INSERT INTO data_lineage (target_table, target_schema, source_table, source_schema, relation_type, join_type, etl_script_path, etl_logic_summary, created_by)
VALUES
('dm.dmm_sac_loan_prod_daily', 'dm', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM', 'sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql', '按产品维度聚合当日放款明细', 'test_user'),
('dm.dmm_sac_loan_prod_daily', 'dm', 'dim.dim_product', 'dim', 'ETL', 'LEFT JOIN', 'sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql', '关联产品维度获取产品名称', 'test_user');

-- 场景2: dm.dmm_sac_loan_chn_daily (贷款渠道日报表)
--   依赖: dwd.dwd_loan_detail, dim.dim_channel
INSERT INTO data_lineage (target_table, target_schema, source_table, source_schema, relation_type, join_type, etl_script_path, etl_logic_summary, created_by)
VALUES
('dm.dmm_sac_loan_chn_daily', 'dm', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM', 'sql/hive/etl/dm/dmm_sac_loan_chn_daily_etl.sql', '按渠道维度聚合当日放款明细', 'test_user'),
('dm.dmm_sac_loan_chn_daily', 'dm', 'dim.dim_channel', 'dim', 'ETL', 'LEFT JOIN', 'sql/hive/etl/dm/dmm_sac_loan_chn_daily_etl.sql', '关联渠道维度获取渠道名称', 'test_user');

-- 场景3: da.da_loan_report (贷款报表 - 依赖 dm 层)
--   依赖: dm.dmm_sac_loan_prod_daily, dm.dmm_sac_loan_chn_daily
INSERT INTO data_lineage (target_table, target_schema, source_table, source_schema, relation_type, join_type, etl_script_path, etl_logic_summary, created_by)
VALUES
('da.da_loan_report', 'da', 'dm.dmm_sac_loan_prod_daily', 'dm', 'ETL', 'FROM', 'sql/hive/etl/da/da_loan_report_etl.sql', '汇总产品维度指标到报表层', 'test_user'),
('da.da_loan_report', 'da', 'dm.dmm_sac_loan_chn_daily', 'dm', 'ETL', 'LEFT JOIN', 'sql/hive/etl/da/da_loan_report_etl.sql', '关联渠道维度指标', 'test_user');

-- 场景4: dwd.dwd_loan_detail (贷款明细 - 依赖 ods 层)
--   依赖: ods.ods_loan_apply, ods.ods_loan_contract
INSERT INTO data_lineage (target_table, target_schema, source_table, source_schema, relation_type, join_type, etl_script_path, etl_logic_summary, created_by)
VALUES
('dwd.dwd_loan_detail', 'dwd', 'ods.ods_loan_apply', 'ods', 'ETL', 'FROM', 'sql/hive/etl/dwd/dwd_loan_detail_etl.sql', '清洗贷款申请数据', 'test_user'),
('dwd.dwd_loan_detail', 'dwd', 'ods.ods_loan_contract', 'ods', 'ETL', 'LEFT JOIN', 'sql/hive/etl/dwd/dwd_loan_detail_etl.sql', '关联合同信息补充放款字段', 'test_user');

-- 场景5: dws.dws_cust_loan_summary (客户贷款汇总)
--   依赖: dwd.dwd_loan_detail
INSERT INTO data_lineage (target_table, target_schema, source_table, source_schema, relation_type, join_type, etl_script_path, etl_logic_summary, created_by)
VALUES
('dws.dws_cust_loan_summary', 'dws', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM', 'sql/hive/etl/dws/dws_cust_loan_summary_etl.sql', '按客户维度汇总贷款信息', 'test_user');

-- ============================================================
-- 4. 插入测试数据 - 字段级血缘
-- ============================================================

-- dm.dmm_sac_loan_prod_daily 的字段血缘
INSERT INTO column_lineage (target_table, target_column, source_table, source_column, transform_type, transform_expr, table_lineage_id)
VALUES
('dm.dmm_sac_loan_prod_daily', 'product_code', 'dwd.dwd_loan_detail', 'product_code', 'DIRECT', 'product_code', 1),
('dm.dmm_sac_loan_prod_daily', 'product_name', 'dim.dim_product', 'product_name', 'DIRECT', 'product_name', 2),
('dm.dmm_sac_loan_prod_daily', 'td_sum_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'SUM', 'SUM(loan_amount)', 1),
('dm.dmm_sac_loan_prod_daily', 'td_cnt_loan', 'dwd.dwd_loan_detail', 'loan_id', 'COUNT', 'COUNT(loan_id)', 1),
('dm.dmm_sac_loan_prod_daily', 'td_diff_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'CUSTOM', 'SUM(loan_amount) - LAG(SUM(loan_amount))', 1);

-- dm.dmm_sac_loan_chn_daily 的字段血缘
INSERT INTO column_lineage (target_table, target_column, source_table, source_column, transform_type, transform_expr, table_lineage_id)
VALUES
('dm.dmm_sac_loan_chn_daily', 'channel_code', 'dwd.dwd_loan_detail', 'channel_code', 'DIRECT', 'channel_code', 3),
('dm.dmm_sac_loan_chn_daily', 'channel_name', 'dim.dim_channel', 'channel_name', 'DIRECT', 'channel_name', 4),
('dm.dmm_sac_loan_chn_daily', 'td_sum_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'SUM', 'SUM(loan_amount)', 3),
('dm.dmm_sac_loan_chn_daily', 'td_cnt_loan', 'dwd.dwd_loan_detail', 'loan_id', 'COUNT', 'COUNT(loan_id)', 3);

-- da.da_loan_report 的字段血缘
INSERT INTO column_lineage (target_table, target_column, source_table, source_column, transform_type, transform_expr, table_lineage_id)
VALUES
('da.da_loan_report', 'total_loan_amt', 'dm.dmm_sac_loan_prod_daily', 'td_sum_loan_amt', 'SUM', 'SUM(td_sum_loan_amt)', 5),
('da.da_loan_report', 'total_loan_cnt', 'dm.dmm_sac_loan_prod_daily', 'td_cnt_loan', 'SUM', 'SUM(td_cnt_loan)', 5);

-- ============================================================
-- 5. 验证数据
-- ============================================================

-- 查看表级血缘
SELECT '=== 表级血缘数据 ===' AS info;
SELECT id, target_table, source_table, join_type, etl_logic_summary
FROM data_lineage
ORDER BY id;

-- 查看字段级血缘
SELECT '=== 字段级血缘数据 ===' AS info;
SELECT id, target_table, target_column, source_table, source_column, transform_type
FROM column_lineage
ORDER BY id;

-- 测试上游查询
SELECT '=== 测试: dm.dmm_sac_loan_prod_daily 的上游依赖 ===' AS info;
SELECT source_table, join_type, etl_logic_summary
FROM data_lineage
WHERE target_table = 'dm.dmm_sac_loan_prod_daily'
  AND is_active = TRUE;

-- 测试下游查询
SELECT '=== 测试: dwd.dwd_loan_detail 的下游影响 ===' AS info;
SELECT target_table, join_type, etl_logic_summary
FROM data_lineage
WHERE source_table = 'dwd.dwd_loan_detail'
  AND is_active = TRUE;

SELECT '=== 初始化完成 ===' AS info;
