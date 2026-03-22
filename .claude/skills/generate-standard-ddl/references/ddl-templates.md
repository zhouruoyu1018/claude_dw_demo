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
    -- 时间前缀示例: today_(当日), yestd_(昨日), curr_mth_(当月), his_(历史⚠️未入库)
    -- 聚合前缀示例(CONVERGE): sum_, avg_, max_, min_, tot_, cum_
    -- 分类后缀示例(CATEGORY_WORD): _amt, _cnt, _days, _bal, _fee, _ytd, _mtd
)
COMMENT '{业务含义}，{更新频率}[粒度:{col1},{col2},stat_date]'
PARTITIONED BY (stat_date STRING COMMENT '数据日期，格式YYYY-MM-DD')
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
    stat_date     STRING    COMMENT '数据日期，格式YYYY-MM-DD',
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
PARTITIONED BY (stat_date STRING COMMENT '数据日期，格式YYYY-MM-DD')
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
    `partition_key`    DATE            COMMENT '数据日期',
    `{dim_col}`        {TYPE}          COMMENT '{注释}',

    -- ===== Value 列（指标，指定聚合方式） =====
    `{metric_col}`     {TYPE}          {AGG_TYPE}    COMMENT '{注释}'
)
ENGINE = OLAP
AGGREGATE KEY (`partition_key`, `{dim_col}`)
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
    `partition_key`    DATE            COMMENT '数据日期',

    -- ===== Value 列 =====
    `{value_col}`      {TYPE}          COMMENT '{注释}'
)
ENGINE = OLAP
UNIQUE KEY (`{pk_col}`, `partition_key`)
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

### 3.3.1 同步表模板（Hive → Doris）

适用于 CASE D：将 Hive 数据定时同步到 Doris 本地表。

```sql
-- =================================================================
-- 同步表名:    {db}.{table_name}
-- 同步来源:    hive_bdsp.{source_db}.{source_table}
-- 表模型:      {Duplicate/Unique}（明细用 Duplicate，有去重需求用 Unique）
-- 同步字段:    仅同步需要的字段，非全量复制
-- 创建时间:    {YYYY-MM-DD}
-- =================================================================

CREATE TABLE IF NOT EXISTS {db}.{table_name} (
    `{col1}`           {DORIS_TYPE}    COMMENT '{注释}',
    `{col2}`           {DORIS_TYPE}    COMMENT '{注释}',
    `partition_key`    DATE            COMMENT '分区键（对应 Hive stat_date）'
)
ENGINE = OLAP
{DUPLICATE KEY / UNIQUE KEY} (`{key_cols}`, `partition_key`)
COMMENT '同步自 hive_bdsp.{source_db}.{source_table}'
DISTRIBUTED BY HASH(`{hash_col}`) BUCKETS {N}
PROPERTIES (
    'replication_num' = '3'
);
```

**同步表生成规则：**
- 表模型默认 Duplicate Model（保留明细），有去重需求用 Unique Model
- 字段类型按 §3.4 映射表转换（STRING→VARCHAR(N), TIMESTAMP→DATETIME 等）
- 分区键统一为 `partition_key DATE`
- 库名固定为 `ph_dm_sac_drs`
- COMMENT 必须标注来源表全名
- 仅同步业务需要的字段，不要求全量复制

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

## 4. 数据类型选择

### 4.1 混合规范策略

本项目根据分层自动选择类型规范：

| 分层 | 采用规范 | 理由 |
|------|---------|------|
| **ODS/DWD** | 规范 B (简化) | 贴近源系统，跨引擎兼容 |
| **DWM/DWS/DIM** | 规范 A (标准) | 性能优化，高频查询 |
| **DM/DA** | 规范 B (简化) | 跨引擎消费与导出 |

### 4.2 规范 A: 标准规范（DWM/DWS/DIM 层）

类型细化，性能优化优先。

| 业务类型 | Hive 类型 | 说明 |
|---------|-----------|------|
| 主键/ID | `BIGINT` | 数字型标识 |
| 编码/名称 | `STRING` | 变长字符 |
| 金额 | `DECIMAL(18,2)` | 精确到分 |
| 件数/笔数 | `BIGINT` | 计数类 |
| 比率/占比 | `DECIMAL(10,4)` | 精确到万分位 |
| 天数 | `INT` | 整数天 |
| 布尔 | `TINYINT` | 0/1 |
| 日期/时间戳 | `STRING` | 格式 YYYY-MM-DD 或 YYYY-MM-DD HH:mm:ss |

### 4.3 规范 B: 简化规范（ODS/DWD/DM/DA 层）

仅 2 种类型，极简易用。

| 业务类型 | Hive 类型 | 说明 |
|---------|-----------|------|
| 所有数值型 | `DECIMAL(38,10)` | 可参与计算、聚合的字段（金额、笔数、比率、天数等） |
| 其他类型 | `STRING` | 编码、名称、日期、布尔等 |

### 4.4 自动识别分层

识别优先级：数据库名 > 表名前缀 > `dw-requirement-triage` 建议，默认 DM 层。

| 数据库/前缀 | 分层 | 物理库名 (Hive/Impala) | 规范 |
|------------|------|----------------------|------|
| `ods` / `ods_` | ODS | _(非工作范围)_ | B |
| `dwd` / `dwd_` | DWD | _(非工作范围)_ | B |
| `dwm` / `dwm_` | DWM | _(非工作范围)_ | A |
| `dws` / `dws_` | DWS | _(非工作范围)_ | A |
| `dm` / `dmm_` / `dm_` | DM | `ph_sac_dmm` | B |
| `da` / `da_` | DA | `ph_sac_da` | B |
| `dim` / `dim_` | DIM | _(非工作范围)_ | A |

### 4.5 通用注意事项

- ✅ 分区字段必须为 `STRING` 类型，格式为 `YYYY-MM-DD` (10 位)
- ✅ 禁止分区字段带时分秒（避免动态分区冒号转义问题）
- ✅ 金额字段必须在 COMMENT 中注明单位（如 `单位：元`）
- ✅ 比率字段必须在 COMMENT 中注明格式（如 `0.0523 表示 5.23%`）
- ✅ 布尔字段使用规范 A 时为 `TINYINT (0/1)`，规范 B 时为 `STRING ('0'/'1')`

---

## 5. COMMENT 规范

每个字段和表都必须有 COMMENT：

| 类型 | 要求 | 示例 |
|------|------|------|
| **表** | 业务含义 + 更新频率 + **末尾粒度声明** | `'贷款产品日维度指标宽表，T+1更新[粒度:product_code,stat_date]'` |
| **维度字段** | 业务含义 | `'产品编码'` |
| **指标字段** | 计算口径 | `'当日放款总金额，单位：元'` |
| **布尔字段** | 必须注明 0/1 含义 | `'是否首次逾期，0-否 1-是'` |
| **枚举字段** | 列出枚举值 | `'还款状态，1-正常 2-逾期 3-核销'` |
| **比率字段** | 注明格式 | `'M1逾期率=M1逾期本金/应还本金，0.0523 表示 5.23%'` |

---

## 6. 完整 DDL 设计示例

**需求：** 按日+产品维度统计放款金额、放款笔数、平均授信额度

| Step | 动作 | 结果 |
|------|------|------|
| 0 | 请求分型 | 用户给需求字段清单 → `full_design` |
| 1 | 确定分层与表名 | dm → `dmm_sac_loan_prod_daily` |
| 2 | 建模决策 | 未找到同粒度同主题表 → CASE B 新建 |
| 3 | 语义拆分→词根查询→assemble→validate | `today_sum_loan_amt`, `today_loan_cnt`, `today_avg_credit_amt` |
| 4 | 排序 + 类型 | DM 层 → 规范 B（DECIMAL(38,10) / STRING） |
| 5 | 生成 DDL | CREATE TABLE + TBLPROPERTIES（含 logical_primary_key） |

---

## 7. 引擎选择速查

| 场景 | 推荐引擎 | DDL 要点 |
|------|---------|---------|
| T+1 批量报表 | Hive | PARQUET + SNAPPY，PARTITIONED BY stat_date |
| 交互式即席查询 | Impala | PARQUET + SNAPPY，从 Hive 同步 |
| 实时大屏/高并发 | Doris | 根据语义选 Aggregate/Unique/Duplicate |
| 需要精确去重 | Doris | BITMAP_UNION 聚合类型 |
| 需要 Upsert | Doris | Unique Model + merge_on_write |
