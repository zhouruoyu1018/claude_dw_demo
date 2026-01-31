# DDL 模板

针对不同查询引擎的标准 DDL 模板。

---

## 1. Hive (Tez) DDL

### 1.1 CREATE TABLE

```sql
-- ============================================================
-- 表名:    {schema}.{table_name}
-- 功能:    {表功能描述}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 初始创建
-- ============================================================

DROP TABLE IF EXISTS {schema}.{table_name};

CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    -- ===== 维度字段 =====
    {dim_col}          {TYPE}          COMMENT '{注释}',

    -- ===== 布尔字段 =====
    {bool_col}         TINYINT         COMMENT '{注释}，0-否 1-是',

    -- ===== 指标字段 =====
    {metric_col}       {TYPE}          COMMENT '{注释}'
)
COMMENT '{业务含义}，{更新频率}[粒度:{col1},{col2},dt]'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'logical_primary_key' = '{col1},{col2},...',
    'business_owner' = '{负责人}',
    'data_layer' = '{dm|da}'
);
```

**必填属性说明：**

| TBLPROPERTIES 键 | 必填 | 说明 |
|------------------|------|------|
| `logical_primary_key` | **是** | 逻辑主键，逗号分隔维度字段+分区字段 |
| `parquet.compression` | 是 | 压缩算法，默认 `SNAPPY` |
| `business_owner` | 是 | 业务归属人或团队 |
| `data_layer` | 是 | 所属分层：`dm` 或 `da` |

### 1.2 ALTER TABLE ADD COLUMNS

```sql
-- ============================================================
-- 表名:    {schema}.{table_name}
-- 变更:    新增 {N} 个字段 — {变更原因}
-- 作者:    {author}
-- 变更日期: {YYYY-MM-DD}
-- ============================================================

ALTER TABLE {schema}.{table_name} ADD COLUMNS (
    {new_col_1}    {TYPE}    COMMENT '{注释}',
    {new_col_2}    {TYPE}    COMMENT '{注释}'
) CASCADE;
```

**注意：分区表必须加 `CASCADE`**，否则新增字段不会应用到已存在的分区，导致旧分区查询新字段返回 NULL。

### 1.3 多级分区

当需要多维度分区时（仅限枚举值少的维度）：

```sql
PARTITIONED BY (
    dt            STRING    COMMENT '数据日期，格式YYYY-MM-DD',
    product_code  STRING    COMMENT '产品编码'
)
```

---

## 2. Impala DDL

Impala 与 Hive 共享 Metastore，通常在 Hive 中建表后通过 `INVALIDATE METADATA` 同步。
如需 Impala 独立建表：

### 2.1 CREATE TABLE

```sql
-- ============================================================
-- 表名:    {schema}.{table_name}
-- 引擎:    Impala
-- 功能:    {表功能描述}
-- 粒度:    {一行 = 什么}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- ============================================================

CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    -- ===== 维度字段 =====
    {dim_col}          {TYPE}          COMMENT '{注释}',

    -- ===== 指标字段 =====
    {metric_col}       {TYPE}          COMMENT '{注释}'
)
COMMENT '{表注释}'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'logical_primary_key' = '{col1},{col2},...'
);
```

### 2.2 Hive 建表后同步

```sql
-- 在 Impala 中执行，使 Impala 感知 Hive 新建的表
INVALIDATE METADATA {schema}.{table_name};

-- 仅刷新分区信息（轻量操作）
REFRESH {schema}.{table_name};
```

**注意事项：**
- Impala 不支持 `SORT BY`、`CLUSTER BY`
- Impala 的 `STRING` 对应 Hive 的 `STRING`，但 Impala 额外支持 `VARCHAR(n)`
- 建议统一在 Hive 建表，Impala 通过 INVALIDATE METADATA 同步

---

## 3. Doris DDL

### 3.1 Aggregate Model（聚合模型）

适用于预聚合指标场景：

```sql
-- ============================================================
-- 表名:    {db}.{table_name}
-- 引擎:    Doris (Aggregate Model)
-- 功能:    {表功能描述}
-- 粒度:    {一行 = 什么}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- ============================================================

CREATE TABLE IF NOT EXISTS {db}.{table_name} (
    -- ===== Key 列（维度） =====
    `dt`               DATE            COMMENT '数据日期',
    `{dim_col}`        {TYPE}          COMMENT '{注释}',

    -- ===== Value 列（指标，指定聚合方式） =====
    `{metric_col}`     {TYPE}          {AGG_TYPE}    COMMENT '{注释}'
)
ENGINE = OLAP
AGGREGATE KEY (`dt`, `{dim_col}`)
COMMENT '{表注释}'
DISTRIBUTED BY HASH(`{dim_col}`) BUCKETS {N}
PROPERTIES (
    'replication_num' = '3',
    'storage_format' = 'V2'
);
```

**聚合类型映射：**

| 业务语义 | Doris AGG_TYPE |
|---------|----------------|
| 累加金额/笔数 | `SUM` |
| 最大值 | `MAX` |
| 最小值 | `MIN` |
| 替换（取最新值） | `REPLACE` |
| 去重计数 (精确) | `BITMAP_UNION` |
| 去重计数 (近似) | `HLL_UNION` |

### 3.2 Unique Model（唯一主键模型）

适用于需要 Upsert 语义的场景：

```sql
CREATE TABLE IF NOT EXISTS {db}.{table_name} (
    -- ===== Key 列（主键） =====
    `{pk_col}`         {TYPE}          COMMENT '{注释}',
    `dt`               DATE            COMMENT '数据日期',

    -- ===== Value 列 =====
    `{value_col}`      {TYPE}          COMMENT '{注释}'
)
ENGINE = OLAP
UNIQUE KEY (`{pk_col}`, `dt`)
COMMENT '{表注释}'
DISTRIBUTED BY HASH(`{pk_col}`) BUCKETS {N}
PROPERTIES (
    'replication_num' = '3',
    'enable_unique_key_merge_on_write' = 'true'
);
```

### 3.3 Duplicate Model（明细模型）

适用于明细日志、全量保留：

```sql
CREATE TABLE IF NOT EXISTS {db}.{table_name} (
    `{col}`            {TYPE}          COMMENT '{注释}'
)
ENGINE = OLAP
DUPLICATE KEY (`{sort_col_1}`, `{sort_col_2}`)
COMMENT '{表注释}'
DISTRIBUTED BY HASH(`{hash_col}`) BUCKETS {N}
PROPERTIES (
    'replication_num' = '3'
);
```

### 3.4 Doris 数据类型映射

| Hive 类型 | Doris 类型 | 说明 |
|-----------|-----------|------|
| `STRING` | `VARCHAR(N)` | Doris 需要指定长度 |
| `BIGINT` | `BIGINT` | 一致 |
| `INT` | `INT` | 一致 |
| `TINYINT` | `TINYINT` | 一致 |
| `DECIMAL(18,2)` | `DECIMAL(18,2)` | 一致 |
| `TIMESTAMP` | `DATETIME` | 类型名不同 |
| `STRING` (日期) | `DATE` | Doris 有原生 DATE |

---

## 4. 引擎选择速查

| 场景 | 推荐引擎 | DDL 要点 |
|------|---------|---------|
| T+1 批量报表 | Hive | PARQUET + SNAPPY，PARTITIONED BY dt |
| 交互式即席查询 | Impala | PARQUET + SNAPPY，从 Hive 同步 |
| 实时大屏/高并发 | Doris | 根据语义选 Aggregate/Unique/Duplicate |
| 需要精确去重 | Doris | BITMAP_UNION 聚合类型 |
| 需要 Upsert | Doris | Unique Model + merge_on_write |
