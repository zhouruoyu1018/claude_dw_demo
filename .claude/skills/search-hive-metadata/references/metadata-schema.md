# Hive 元数据表结构

## tbl_base_info 表

存储 Hive 表的基础元数据信息。

```sql
CREATE TABLE `tbl_base_info` (
  `table_name_full` varchar(512) COMMENT 'hive表全名称，如 schema.table_name',
  `table_comment` varchar(1024) DEFAULT NULL COMMENT '表注释',
  `schema_name` varchar(128) DEFAULT NULL COMMENT '数据库名',
  `table_name` varchar(256) DEFAULT NULL COMMENT '表名（不含库名）',
  `column_list` longtext DEFAULT NULL COMMENT '字段清单（JSON格式）',
  `partition_key` varchar(512) DEFAULT NULL COMMENT '分区字段',
  `column_cnt` int DEFAULT NULL COMMENT '字段数量',
  `partition_cnt` int DEFAULT NULL COMMENT '分区数量',
  `file_cnt` int DEFAULT NULL COMMENT '文件数量',
  `total_data_size_display` varchar(64) DEFAULT NULL COMMENT '总数据量展示',
  `tbl_type` varchar(64) DEFAULT NULL COMMENT '表类型',
  `tbl_row_cnt` bigint DEFAULT NULL COMMENT '表行数',
  `tbl_strg_format` varchar(64) DEFAULT NULL COMMENT '存储格式',
  `partition_list` longtext DEFAULT NULL COMMENT '分区列表（JSON格式）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='hive表基础信息表';
```

## 字段说明

| 字段名 | 类型 | 说明 | 示例 |
|-------|------|-----|------|
| table_name_full | varchar(512) | 表全名 | `ph_dwm.dwm_mod_loan_base` |
| table_comment | varchar(1024) | 表注释 | `贷款基础信息表` |
| schema_name | varchar(128) | 数据库名 | `ph_ods`, `ph_dwd`, `ph_dwm` |
| table_name | varchar(256) | 表名（不含库名） | `dwm_mod_loan_base` |
| column_list | longtext | 字段列表（JSON格式） | 见下方示例 |
| partition_key | varchar(512) | 分区字段 | `[src_sys]` |
| column_cnt | int | 字段数量 | `70` |
| partition_cnt | int | 分区数量 | `6` |
| file_cnt | int | 文件数量 | `456` |
| total_data_size_display | varchar(64) | 数据量展示 | `11.39 GB` |
| tbl_type | varchar(64) | 表类型 | `TABLE`, `VIEW` |
| tbl_row_cnt | bigint | 表行数 | `127902872` |
| tbl_strg_format | varchar(64) | 存储格式 | `PARQUET`, `ORC`, `TEXTFILE` |
| partition_list | longtext | 分区列表（JSON格式） | 见下方示例 |

## column_list JSON 格式

字段列表以 JSON 对象格式存储：

```json
{
  "columns": [
    {
      "id": "0001",
      "column_name": "apply_no",
      "type_name": "string",
      "comment_name": "申请号"
    },
    {
      "id": "0002",
      "column_name": "loan_no",
      "type_name": "string",
      "comment_name": "贷款号"
    },
    {
      "id": "0003",
      "column_name": "loan_amount",
      "type_name": "decimal(18,2)",
      "comment_name": "贷款金额"
    }
  ]
}
```

**注意**：MCP Server 的 `parse_column_list` 函数会自动将上述格式转换为统一格式：

```json
[
  {"name": "apply_no", "type": "string", "comment": "申请号"},
  {"name": "loan_no", "type": "string", "comment": "贷款号"},
  {"name": "loan_amount", "type": "decimal(18,2)", "comment": "贷款金额"}
]
```

## partition_list JSON 格式

分区列表以 JSON 对象格式存储：

```json
{
  "partitions": [
    {
      "partition_value": "src_sys=biz_sys",
      "numfiles": "1",
      "numrows": "30157",
      "rawdatasize": ""
    },
    {
      "partition_value": "src_sys=cfods_sys",
      "numfiles": "133",
      "numrows": "2174822",
      "rawdatasize": ""
    }
  ]
}
```

---

## data_lineage 表（PostgreSQL）

存储表级数据血缘关系。

```sql
CREATE TABLE IF NOT EXISTS data_lineage (
    id SERIAL PRIMARY KEY,

    -- 目标表信息
    target_table VARCHAR(200) NOT NULL COMMENT '目标表完整名，如 dm.dmm_sac_loan_prod_daily',
    target_schema VARCHAR(100) COMMENT '目标表所属库',

    -- 源表信息
    source_table VARCHAR(200) NOT NULL COMMENT '源表完整名',
    source_schema VARCHAR(100) COMMENT '源表所属库',

    -- 关系类型
    relation_type VARCHAR(50) DEFAULT 'ETL' COMMENT '关系类型: ETL/VIEW/MANUAL',
    join_type VARCHAR(50) COMMENT 'JOIN 类型: INNER/LEFT/RIGHT/FULL/CROSS',

    -- ETL 信息
    etl_script_path VARCHAR(500) COMMENT 'ETL 脚本路径',
    etl_logic_summary VARCHAR(1000) COMMENT 'ETL 逻辑摘要，如 "聚合放款明细到产品日维度"',

    -- 元信息
    created_by VARCHAR(100) DEFAULT 'auto',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否有效（表删除后置为 FALSE）'
);

-- 索引：按目标表查上游
CREATE INDEX idx_lineage_target ON data_lineage(target_table);
-- 索引：按源表查下游
CREATE INDEX idx_lineage_source ON data_lineage(source_table);
```

## column_lineage 表（PostgreSQL，可选）

存储字段级血缘关系，支持精细化影响分析。

```sql
CREATE TABLE IF NOT EXISTS column_lineage (
    id SERIAL PRIMARY KEY,

    -- 目标字段
    target_table VARCHAR(200) NOT NULL,
    target_column VARCHAR(200) NOT NULL,

    -- 源字段
    source_table VARCHAR(200) NOT NULL,
    source_column VARCHAR(200) NOT NULL,

    -- 转换逻辑
    transform_type VARCHAR(50) COMMENT '转换类型: DIRECT/SUM/COUNT/AVG/MAX/MIN/CASE/CUSTOM',
    transform_expr VARCHAR(1000) COMMENT '转换表达式，如 SUM(loan_amount)',

    -- 关联的表级血缘
    table_lineage_id INT REFERENCES data_lineage(id),

    -- 元信息
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX idx_col_lineage_target ON column_lineage(target_table, target_column);
CREATE INDEX idx_col_lineage_source ON column_lineage(source_table, source_column);
```

## 血缘数据示例

### 表级血缘

```sql
INSERT INTO data_lineage (target_table, source_table, relation_type, join_type, etl_logic_summary)
VALUES
('dm.dmm_sac_loan_prod_daily', 'dwd.dwd_loan_detail', 'ETL', 'FROM', '按产品维度聚合放款明细'),
('dm.dmm_sac_loan_prod_daily', 'dim.dim_product', 'ETL', 'LEFT JOIN', '关联产品维度获取产品名称');
```

### 字段级血缘

```sql
INSERT INTO column_lineage (target_table, target_column, source_table, source_column, transform_type, transform_expr)
VALUES
('dm.dmm_sac_loan_prod_daily', 'product_code', 'dwd.dwd_loan_detail', 'product_code', 'DIRECT', 'product_code'),
('dm.dmm_sac_loan_prod_daily', 'product_name', 'dim.dim_product', 'product_name', 'DIRECT', 'product_name'),
('dm.dmm_sac_loan_prod_daily', 'td_sum_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'SUM', 'SUM(loan_amount)'),
('dm.dmm_sac_loan_prod_daily', 'td_cnt_loan', 'dwd.dwd_loan_detail', 'loan_id', 'COUNT', 'COUNT(loan_id)');
```

## 常用血缘查询

### 查询某表的上游依赖（我依赖谁）

```sql
SELECT source_table, join_type, etl_logic_summary
FROM data_lineage
WHERE target_table = 'dm.dmm_sac_loan_prod_daily'
  AND is_active = TRUE;
```

### 查询某表的下游影响（谁依赖我）

```sql
SELECT target_table, join_type, etl_logic_summary
FROM data_lineage
WHERE source_table = 'dwd.dwd_loan_detail'
  AND is_active = TRUE;
```

### 查询某字段的来源（字段溯源）

```sql
SELECT source_table, source_column, transform_type, transform_expr
FROM column_lineage
WHERE target_table = 'dm.dmm_sac_loan_prod_daily'
  AND target_column = 'td_sum_loan_amt';
```

### 查询某字段的影响范围（字段影响分析）

```sql
SELECT target_table, target_column, transform_type
FROM column_lineage
WHERE source_table = 'dwd.dwd_loan_detail'
  AND source_column = 'loan_amount';
```

## schema_name 数仓分层

| 分层 | 说明 | 示例 |
|-----|------|------|
| ods | 原始数据层 | `ods_order_info` |
| dwd | 明细数据层 | `dwd_order_detail` |
| dws | 汇总数据层 | `dws_order_daily` |
| ads | 应用数据层 | `ads_order_analysis` |
| dim | 维度表 | `dim_user`, `dim_product` |
| tmp | 临时表 | `tmp_order_calc` |

## 常用查询

### 按数据库查询表数量

```sql
SELECT schema_name, COUNT(*) as table_count
FROM tbl_base_info
WHERE is_deleted = 0
GROUP BY schema_name
ORDER BY table_count DESC;
```

### 查询大表 Top 10

```sql
SELECT table_name_full, total_data_size_display, owner
FROM tbl_base_info
WHERE is_deleted = 0
ORDER BY
  CASE
    WHEN total_data_size_display LIKE '%TB%' THEN CAST(REPLACE(total_data_size_display, 'TB', '') AS DECIMAL) * 1024
    WHEN total_data_size_display LIKE '%GB%' THEN CAST(REPLACE(total_data_size_display, 'GB', '') AS DECIMAL)
    ELSE 0
  END DESC
LIMIT 10;
```

### 模糊搜索表

```sql
SELECT table_name_full, table_comment
FROM tbl_base_info
WHERE is_deleted = 0
  AND (table_name_full LIKE '%order%' OR table_comment LIKE '%订单%')
ORDER BY update_time DESC
LIMIT 20;
```

---

## indicator_registry 表

存储已计算好的业务指标信息，支持"复用优先"策略。

```sql
CREATE TABLE IF NOT EXISTS indicator_registry (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    indicator_name VARCHAR(50) NOT NULL COMMENT '指标名',
    target_column VARCHAR(50) DEFAULT '' COMMENT '对应物理字段',
    source_table VARCHAR(100) NOT NULL COMMENT '所在表',
    logic_desc VARCHAR(200) NOT NULL COMMENT '口径描述',
    remarks VARCHAR(100) DEFAULT '' COMMENT '备注',
    created_by VARCHAR(50) NULL COMMENT '创建人',
    created_time DATETIME NULL COMMENT '创建日期',
    updated_by VARCHAR(50) NULL COMMENT '更新人',
    updated_time DATETIME NULL COMMENT '更新日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指标库表';
```

## 指标库字段说明

| 字段名 | 类型 | 约束 | 说明 | 示例 |
|-------|------|------|-----|------|
| indicator_code | varchar(50) | 必填，唯一 | 指标编码 | `IDX_LOAN_001` |
| indicator_name | varchar(50) | 必填，唯一 | 业务指标名称 | `当日放款金额` |
| indicator_english_name | varchar(50) | 必填 | 英文名/物理字段名 | `td_loan_amt` |
| indicator_alias | varchar(50) | 可选 | 指标别名 | `放款额` |
| indicator_category | varchar(20) | 必填 | 指标分类 | `原子指标`/`派生指标`/`复合指标` |
| business_domain | varchar(20) | 必填 | 业务域 | `贷款`/`风控`/`营销` |
| statistical_caliber | varchar(200) | 可选 | 业务口径描述 | `当日实际放款成功的金额合计` |
| calculation_logic | text | 可选（推荐填写） | 取值逻辑，推荐格式: `SELECT 字段 FROM 表 WHERE 条件` | `SELECT SUM(loan_amt) FROM dwd.dwd_loan_dtl WHERE status='SUCCESS'` |
| data_source | varchar(100) | 可选 | 数据来源表 | `dm.dmm_sac_loan_prod_daily` |
| data_type | varchar(20) | 必填，枚举 | 从元数据获取的物理字段类型。可选值: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `STRING`, `VARCHAR`, `CHAR`, `DATE`, `TIMESTAMP`, `BOOLEAN`, `ARRAY`, `MAP`, `STRUCT` | `DECIMAL` |
| standard_type | varchar(10) | 必填，枚举 | 标准类型（逻辑分类）。可选值: `数值类`, `日期类`, `文本类`, `枚举类`, `时间类` | `数值类` |
| update_frequency | varchar(10) | 必填，枚举 | 更新频率。可选值: `实时`, `每小时`, `每日`, `每周`, `每月`, `每季`, `每年`, `手动` | `每日` |
| status | varchar(10) | 必填，枚举 | 状态。可选值: `启用`, `未启用`, `废弃`，默认 `启用` | `启用` |
| it_owner | varchar(50) | 可选 | IT 负责人 | `zhangsan` |
| business_owner | varchar(50) | 可选 | 业务负责人 | `lisi` |
| create_time | datetime | 自动 | 创建时间 | |
| update_time | datetime | 自动 | 更新时间 | |

## 指标库示例数据

```sql
INSERT INTO indicator_registry
(indicator_name, target_column, source_table, logic_desc, remarks, created_by, created_time)
VALUES
('日销售额', 'gmv', 'dws.dws_shop_1d', '包含退款的下单金额', '', 'zhangsan', NOW()),
('复购率', 'repurchase_rate', 'ads.ads_user_retention_1d', '统计周期内发生2次及以上支付行为的用户占比', '', 'lisi', NOW()),
('客单价', 'avg_order_amount', 'dws.dws_order_daily', '日销售额/支付订单数', '仅统计已支付订单', 'wangwu', NOW());
```

## 指标库常用查询

### 按指标名模糊搜索

```sql
SELECT indicator_name, target_column, source_table, logic_desc
FROM indicator_registry
WHERE indicator_name LIKE '%复购%'
   OR logic_desc LIKE '%复购%';
```

### 查询某表包含的指标

```sql
SELECT indicator_name, target_column, logic_desc
FROM indicator_registry
WHERE source_table = 'ads.ads_user_retention_1d';
```
