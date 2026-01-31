-- ============================================================
-- Author:
-- Create Date:
-- Description: Example ODS layer table definition
-- Changelog:
--   YYYY-MM-DD - Initial creation
-- ============================================================

CREATE TABLE IF NOT EXISTS ods_example_table (
    id              BIGINT      COMMENT 'Primary key',
    name            STRING      COMMENT 'Name field',
    amount          DECIMAL(18,2) COMMENT 'Amount',
    created_at      TIMESTAMP   COMMENT 'Record creation time',
    updated_at      TIMESTAMP   COMMENT 'Record update time'
)
COMMENT 'Example ODS table'
PARTITIONED BY (dt STRING COMMENT 'Partition date YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);
