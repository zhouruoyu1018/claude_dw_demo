# SQL 审查规则详细清单

本文档包含每条审查规则的**详细判断逻辑、正反示例和边界条件**。

---

## A. DDL 审查规则

### D-01: 表名命名规范 (ERROR)

**规则**: 表名必须符合 `{层前缀}_{业务主题}_{粒度}` 格式。

**判断逻辑**:
1. 提取 `CREATE TABLE` 后的表名（含 schema）
2. 检查是否包含合法前缀：`dmm_sac_`、`da_sac_`、`dwd_`、`dws_`、`ods_`、`dim_`
3. 检查是否包含业务主题词（apply/credit/sign/loan/repay/overdue/collect/writeoff）
4. 检查是否包含粒度后缀（_daily/_weekly/_monthly/_dtl）或合理的省略

**正例**:
```sql
CREATE TABLE dm.dmm_sac_loan_prod_daily (...)    -- dm 层，放款主题，产品+日粒度
CREATE TABLE da.da_sac_overdue_analysis (...)     -- da 层，逾期主题
```

**反例**:
```sql
CREATE TABLE dm.loan_report_v2 (...)              -- 缺少分层前缀
CREATE TABLE dm.dmm_sac_table1 (...)              -- 缺少业务主题
CREATE TABLE dm.DMM_SAC_LOAN_DAILY (...)          -- 大写（应 snake_case）
```

---

### D-02: 字段名词根合规 (WARN)

**规则**: 字段名应基于词根表组装，遵循 `{BOOL}_{TIME}_{CONVERGE}_{BIZ_ENTITY}_{CATEGORY_WORD}` 顺序。

**判断逻辑**:
1. 拆解字段名各段
2. 检查常用词根是否匹配（amt/cnt/rat/sum/avg/max/min/loan/repay/overdue 等）
3. 检查组装顺序是否正确（如 `is_` 在最前，`td_`/`his_` 在 `sum_`/`cnt_` 前）

**正例**:
```sql
td_sum_loan_amt       -- td(TIME) + sum(CONVERGE) + loan(BIZ) + amt(CATEGORY)
is_first_overdue      -- is(BOOL) + first(BIZ) + overdue(BIZ)
his_max_overdue_days  -- his(TIME) + max(CONVERGE) + overdue(BIZ) + days(CATEGORY)
```

**反例**:
```sql
loan_amt_sum_td       -- 顺序错误，应为 td_sum_loan_amt
total_amount          -- 未使用词根，应为 sum_loan_amt 或 td_sum_loan_amt
cnt_sum_loan          -- CONVERGE 重复，sum 和 cnt 不应同时出现
```

**参考**: `generate-standard-ddl/references/naming-convention.md`

---

### D-03: 字段 COMMENT 完整性 (ERROR)

**规则**: DDL 中的每个字段都必须有 COMMENT。

**判断逻辑**:
1. 解析每个字段定义行
2. 检查是否包含 `COMMENT '...'`
3. COMMENT 内容不能为空串

**正例**:
```sql
product_code    STRING      COMMENT '产品编码',
td_sum_loan_amt DECIMAL(18,2) COMMENT '当日放款总金额，单位：元',
```

**反例**:
```sql
product_code    STRING,                           -- 无 COMMENT
td_sum_loan_amt DECIMAL(18,2) COMMENT '',         -- 空 COMMENT
```

**额外检查**:
- 金额字段 COMMENT 应包含"单位"
- 比率字段 COMMENT 应包含格式说明（如"0.0523 表示 5.23%"）
- 布尔字段 COMMENT 应包含"0-否 1-是"

---

### D-04: 表 COMMENT 粒度标注 (WARN)

**规则**: 表 COMMENT 末尾应包含粒度声明 `[粒度:col1,col2]`。

**判断逻辑**:
1. 找到 `COMMENT '...'`（表级别）
2. 检查是否包含 `[粒度:` 或 `[粒度：`

**正例**:
```sql
COMMENT '贷款产品日维度指标宽表，T+1更新[粒度:product_code,stat_date]'
```

**反例**:
```sql
COMMENT '贷款产品日维度指标宽表'                   -- 缺少粒度标注
COMMENT '贷款产品日维度指标宽表，粒度为产品+日期'   -- 非标准格式
```

---

### D-05: TBLPROPERTIES 完整性 (WARN)

**规则**: Hive 表应包含 TBLPROPERTIES，至少声明 `logical_primary_key`。

**判断逻辑**:
1. 检查是否存在 `TBLPROPERTIES` 块
2. 检查是否包含 `logical_primary_key`
3. 可选检查：`business_owner`、`data_layer`

**正例**:
```sql
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'logical_primary_key' = 'product_code,stat_date'
);
```

**反例**:
```sql
-- 整个 TBLPROPERTIES 缺失
-- 或有 TBLPROPERTIES 但缺少 logical_primary_key
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
```

---

### D-06: 分区字段命名 (WARN)

**规则**: 分区字段应使用标准名称。

**判断逻辑**:
1. 找到 `PARTITIONED BY` 子句
2. 日分区应命名为 `stat_date`（Hive/Impala），Doris 统一使用 `partition_key`
3. 月分区应命名为 `stat_month`
4. 分区字段类型必须为 `STRING`

**正例**:
```sql
PARTITIONED BY (stat_date STRING COMMENT '数据日期')
PARTITIONED BY (stat_date STRING COMMENT '统计日期')
```

**反例**:
```sql
PARTITIONED BY (p_date STRING)        -- 非标准名称
PARTITIONED BY (stat_date INT)         -- 类型应为 STRING
PARTITIONED BY (data_date VARCHAR(10))-- 非标准名称和类型
```

---

### D-07: 字段排序规范 (INFO)

**规则**: 字段应按"维度 → 布尔 → 指标"顺序排列。

**判断逻辑**:
1. 将字段分为三组：维度（_id/_code/_name/_date/_status）、布尔（is_/has_）、指标（其余）
2. 检查是否按组排列
3. 组内是否有注释分隔（`-- ===== 维度字段 =====`）

**参考**: `generate-standard-ddl/references/naming-convention.md` 第 4 节

---

### D-08: 数据类型合理性 (WARN)

**规则**: 数据类型应与业务含义匹配。

**判断逻辑（DM 层 / 规范 A）**:

| 字段特征 | 期望类型 |
|---------|---------|
| 字段名含 `_amt` / `_bal` / `_prin` | `DECIMAL(18,2)` |
| 字段名含 `_cnt` | `BIGINT` |
| 字段名含 `_rat` / `_rate` | `DECIMAL(10,4)` |
| 字段名以 `is_` / `has_` 开头 | `TINYINT` |
| 字段名含 `_days` | `INT` |
| 字段名含 `_id` | `BIGINT` 或 `STRING` |

**判断逻辑（DA 层 / 规范 B）**:

| 字段特征 | 期望类型 |
|---------|---------|
| 可参与计算/聚合 | `DECIMAL(38,10)` |
| 其他 | `STRING` |

---

## B. ETL SQL 审查规则

### E-01: 主表分区过滤 (FATAL)

**规则**: 主表 WHERE 必须包含分区字段过滤条件。

**判断逻辑**:
1. 找到 `FROM` 子句的主表
2. 检查 `WHERE` 子句是否包含 `stat_date = ` / `stat_month = ` / `partition_key = ` 等分区过滤
3. CTE 内的 FROM 也需检查

**正例**:
```sql
FROM dwd.dwd_loan_detail src
WHERE src.stat_date = '${stat_date}'
```

**反例**:
```sql
FROM dwd.dwd_loan_detail src
WHERE src.loan_status = 'SUCCESS'    -- 有 WHERE 但缺少分区过滤
-- 或完全没有 WHERE 子句
```

**例外**: 维度表（dim_ 前缀）通常无分区，无需此检查。

---

### E-02: JOIN 分区过滤 (ERROR)

**规则**: JOIN 的事实表/快照表需带分区过滤条件。

**判断逻辑**:
1. 找到所有 `JOIN` 子句
2. 检查被 JOIN 的表是否为有分区的事实表（dwd_/dws_/dwm_ 前缀）
3. 如果是，ON 条件或 WHERE 中是否包含分区过滤

**正例**:
```sql
LEFT JOIN dwd.dwd_repay_detail repay
    ON src.loan_id = repay.loan_id
    AND repay.stat_date = '${stat_date}'  -- JOIN 条件中带分区过滤
```

**反例**:
```sql
LEFT JOIN dwd.dwd_repay_detail repay
    ON src.loan_id = repay.loan_id   -- 缺少分区过滤，可能全表扫描
```

**例外**: 维度表（dim_ 前缀）无需分区过滤。但需确认维度表确实无分区。

---

### E-03: N:N JOIN 风险 (ERROR)

**规则**: 检查是否存在可能的 N:N JOIN 导致数据膨胀。

**判断逻辑**:
1. 分析 JOIN 键是否为某一方的主键/唯一键
2. 如果两侧都不是唯一键，标记 N:N 风险
3. 自关联（同表 JOIN）尤其需要关注

**风险信号**:
- JOIN 两侧表粒度相同且非唯一键
- 使用 `CROSS JOIN`
- JOIN 条件过于宽泛（如仅按日期 JOIN）

**正例**:
```sql
-- N:1 关联（明细 JOIN 维度表）
FROM dwd.dwd_loan_detail src         -- 粒度: loan_id
LEFT JOIN dim.dim_product dim        -- 粒度: product_code (唯一)
    ON src.product_code = dim.product_code
```

**反例**:
```sql
-- N:N 风险（两张明细表直接 JOIN）
FROM dwd.dwd_loan_detail loan
JOIN dwd.dwd_repay_detail repay
    ON loan.cust_id = repay.cust_id  -- 一个客户可能有多笔贷款和多笔还款
```

---

### E-04: 分母为零处理 (ERROR)

**规则**: 除法运算的分母必须处理为零的情况。

**判断逻辑**:
1. 搜索除法运算符 `/`
2. 检查分母是否有 `NULLIF(..., 0)` 或 `CASE WHEN ... = 0 THEN NULL` 保护
3. 比率字段（_rat/_rate）尤其关注

**正例**:
```sql
-- 方式 1: NULLIF
a.td_sum_loan_amt / NULLIF(b.total_amt, 0) AS rat_loan

-- 方式 2: CASE WHEN
CASE WHEN b.total_amt = 0 THEN NULL
     ELSE a.td_sum_loan_amt / b.total_amt
END AS rat_loan
```

**反例**:
```sql
a.td_sum_loan_amt / b.total_amt AS rat_loan   -- 分母可能为 0
```

---

### E-05: 聚合字段 NULL 处理 (WARN)

**规则**: 聚合字段建议使用 COALESCE 处理 NULL。

**判断逻辑**:
1. 找到 `SUM`/`COUNT`/`AVG` 等聚合函数
2. 检查聚合结果在 SELECT 中是否有 `COALESCE` 兜底
3. LEFT JOIN 后的字段尤其需要 COALESCE

**正例**:
```sql
COALESCE(SUM(src.loan_amount), 0) AS td_sum_loan_amt
```

**注意**: `COUNT(*)` 不会返回 NULL，无需 COALESCE。`COUNT(col)` 在 col 全为 NULL 时返回 0，通常也无需处理。重点关注 `SUM` 和 `AVG`。

---

### E-06: GROUP BY 一致性 (FATAL)

**规则**: GROUP BY 字段必须与 SELECT 中的非聚合字段完全一致。

**判断逻辑**:
1. 提取 SELECT 中所有非聚合字段（不在 SUM/COUNT/AVG/MAX/MIN/窗口函数 中的字段）
2. 提取 GROUP BY 字段列表
3. 两者必须一致（可忽略别名和表前缀差异）

**正例**:
```sql
SELECT
    src.product_code,
    dim.product_name,
    SUM(src.loan_amount) AS td_sum_loan_amt
FROM ...
GROUP BY
    src.product_code,
    dim.product_name
```

**反例**:
```sql
SELECT
    src.product_code,
    dim.product_name,             -- 未在 GROUP BY 中
    SUM(src.loan_amount) AS td_sum_loan_amt
FROM ...
GROUP BY src.product_code         -- 缺少 dim.product_name
```

---

### E-07: 窗口函数完整性 (ERROR)

**规则**: 窗口函数必须有 PARTITION BY（除非全局排序）和 ORDER BY。

**判断逻辑**:
1. 找到 `OVER (` 关键字
2. 检查 `(` 和 `)` 之间是否有 `PARTITION BY`
3. 排名函数（ROW_NUMBER/RANK/DENSE_RANK）必须有 `ORDER BY`
4. 累计函数（SUM OVER）建议有 `ORDER BY` 和 `ROWS BETWEEN`

**正例**:
```sql
ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY loan_date DESC) AS rn
SUM(loan_amount) OVER (PARTITION BY cust_id ORDER BY loan_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_amt
```

**反例**:
```sql
ROW_NUMBER() OVER () AS rn                   -- 缺少 PARTITION BY 和 ORDER BY
SUM(loan_amount) OVER (ORDER BY loan_date)   -- 缺少 PARTITION BY，全表计算
```

---

### E-08: 日期参数化 (ERROR)

**规则**: 日期不应硬编码，应使用参数化变量。

**判断逻辑**:
1. 搜索日期格式的硬编码字符串（`'2024-01-01'`、`'20240101'` 等）
2. 检查是否在 WHERE 条件中
3. COMMENT 中的日期不算硬编码

**正例**:
```sql
WHERE stat_date = '${stat_date}'             -- Hive: '${hivevar:stat_date}'
WHERE stat_date = '${var:stat_date}'         -- Impala
```

**反例**:
```sql
WHERE stat_date = '2024-01-15'               -- 硬编码日期
WHERE stat_date >= '2024-01-01' AND stat_date <= '2024-01-31'  -- 硬编码日期范围
```

**例外**: 初始化脚本的 `WHERE stat_date BETWEEN '${start_dt}' AND '${end_dt}'` 是合规的参数化写法。

---

### E-09: CTE 命名规范 (INFO)

**规则**: CTE 名称应有业务含义，遵循 `base`/`agg_`/`win_`/`dim_`/`final` 命名模式。

**判断逻辑**:
1. 找到 `WITH` 子句中的 CTE 名称
2. 检查是否有业务含义

**正例**:
```sql
WITH base AS (...),
     agg_daily AS (...),
     win_rank AS (...),
     dim_product AS (...)
```

**反例**:
```sql
WITH t1 AS (...),
     t2 AS (...),
     tmp AS (...)
```

---

### E-10: 引擎语法匹配 (WARN)

**规则**: SQL 语法应与目标引擎匹配。

**判断逻辑**:

| 检查项 | Hive | Impala | Doris |
|--------|------|--------|-------|
| 日期加减 | `DATE_ADD(stat_date, N)` | `DAYS_ADD(stat_date, N)` | `DATE_ADD(partition_key, INTERVAL N DAY)` |
| 参数写法 | `${hivevar:stat_date}` | `${var:stat_date}` | 应用层传参 |
| INSERT 语法 | `INSERT OVERWRITE TABLE` | `INSERT OVERWRITE` | `INSERT INTO` |
| GROUPING_ID | `GROUPING__ID`（双下划线） | `GROUPING_ID()` | `GROUPING_ID()` |

**参考**: `generate-etl-sql/references/engine-syntax.md`

---

## C. 规范审查规则

### S-01: 脚本头部注释 (WARN)

**规则**: 脚本应有标准头部注释块。

**判断逻辑**:
1. 检查脚本开头（前 20 行内）是否有注释块
2. 注释块应包含：脚本名称、功能描述、目标表、源表、作者、创建日期

**正例**:
```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_etl.sql
-- 功能:    加工贷款产品日维度指标宽表
-- 目标表:  dm.dmm_sac_loan_prod_daily
-- 源表:    dwd.dwd_loan_detail, dim.dim_product
-- 作者:    zhangsan
-- 创建日期: 2026-01-27
-- ============================================================
```

**反例**:
```sql
-- 放款日报表 ETL                                 -- 过于简略
INSERT OVERWRITE TABLE ...                       -- 直接开始，无头部注释
```

---

### S-02: 层级范围检查 (ERROR)

**规则**: 本项目工作范围仅限 dm/da 层，INSERT 的目标表应在这两层。

**判断逻辑**:
1. 找到 `INSERT OVERWRITE TABLE` / `INSERT INTO` 的目标表
2. 检查表名前缀或 schema 是否为 dm/da

**正例**:
```sql
INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily ...
INSERT INTO da.da_sac_overdue_report ...
```

**反例**:
```sql
INSERT OVERWRITE TABLE dwd.dwd_loan_detail ...   -- dwd 层不在工作范围
INSERT INTO ods.ods_raw_data ...                 -- ods 层不在工作范围
```

**例外**: 如果用户明确说明是跨层操作，可标注为 INFO 而非 ERROR。

---

### S-03: SELECT * 禁用 (ERROR)

**规则**: 禁止在 ETL 脚本中使用 `SELECT *`。

**判断逻辑**:
1. 搜索 `SELECT *` 或 `SELECT src.*`
2. 仅在最终 INSERT 相关的 SELECT 中检查，临时调试用途例外

**正例**:
```sql
SELECT
    src.product_code,
    src.product_name,
    SUM(src.loan_amount) AS td_sum_loan_amt
FROM ...
```

**反例**:
```sql
INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily
SELECT * FROM staging_table;                      -- 字段映射不可控
```

---

### S-04: GROUP BY 列序号 (WARN)

**规则**: GROUP BY 应使用完整字段名，不使用列序号。

**判断逻辑**:
1. 找到 `GROUP BY` 子句
2. 检查是否包含纯数字（如 `GROUP BY 1, 2, 3`）

**正例**:
```sql
GROUP BY
    src.product_code,
    dim.product_name
```

**反例**:
```sql
GROUP BY 1, 2                                    -- 不可读，维护困难
```
