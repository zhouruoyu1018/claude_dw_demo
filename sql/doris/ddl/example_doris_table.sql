-- ============================================================
-- Author:
-- Create Date:
-- Description: Example Doris table definitions for each model type
-- Changelog:
--   YYYY-MM-DD - Initial creation
-- ============================================================

-- Aggregate Model: for pre-aggregated metrics
CREATE TABLE IF NOT EXISTS ads_metrics_agg (
    dt              DATE            COMMENT 'Date',
    category_id     INT             COMMENT 'Category ID',
    pv              BIGINT SUM      COMMENT 'Page views',
    uv              BITMAP BITMAP_UNION COMMENT 'Unique visitors',
    revenue         DECIMAL(18,2) SUM COMMENT 'Total revenue'
)
AGGREGATE KEY(dt, category_id)
DISTRIBUTED BY HASH(category_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- Unique Model: for upsert scenarios
CREATE TABLE IF NOT EXISTS dwd_user_unique (
    user_id         BIGINT          COMMENT 'User ID',
    username        VARCHAR(100)    COMMENT 'Username',
    email           VARCHAR(200)    COMMENT 'Email',
    updated_at      DATETIME        COMMENT 'Last update time'
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- Duplicate Model: for detailed logs
CREATE TABLE IF NOT EXISTS ods_log_detail (
    log_time        DATETIME        COMMENT 'Log timestamp',
    user_id         BIGINT          COMMENT 'User ID',
    action          VARCHAR(50)     COMMENT 'Action type',
    payload         STRING          COMMENT 'Log payload'
)
DUPLICATE KEY(log_time)
PARTITION BY RANGE(log_time) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3"
);
