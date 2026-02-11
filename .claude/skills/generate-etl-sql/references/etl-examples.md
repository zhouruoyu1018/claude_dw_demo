# ETL 完整示例

## 1. 增量脚本示例

**需求：** 按日+产品维度统计放款金额、放款笔数、日环比放款金额

**源表：** `dwd.dwd_loan_detail` (loan_id, product_code, loan_amount, loan_date, stat_date)

**目标表：** `dm.dmm_sac_loan_prod_daily` (product_code, product_name, td_sum_loan_amt, td_cnt_loan, td_diff_loan_amt, stat_date)

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_etl.sql
-- 功能:    加工贷款产品日维度指标宽表
-- 目标表:  dm.dmm_sac_loan_prod_daily
-- 源表:    dwd.dwd_loan_detail, dim.dim_product
-- 粒度:    一行 = 一天 × 一产品
-- 调度:    每日 T+1
-- 依赖:    dwd.dwd_loan_detail (stat_date=${stat_date}), dim.dim_product
-- 作者:    auto-generated
-- 创建日期: 2026-01-27
-- 修改记录:
--   2026-01-27 auto-generated 初始创建
-- ============================================================

-- === Hive 执行参数 ===
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

-- === ETL 主逻辑 ===
WITH
-- CTE 1: 当日放款聚合
agg AS (
    SELECT
        src.product_code,
        SUM(src.loan_amount)             AS td_sum_loan_amt,
        COUNT(src.loan_id)               AS td_cnt_loan
    FROM dwd.dwd_loan_detail src
    WHERE src.stat_date = '${hivevar:stat_date}'
    GROUP BY src.product_code
),
-- CTE 2: 昨日放款金额（用于环比计算）
agg_prev AS (
    SELECT
        src.product_code,
        SUM(src.loan_amount)             AS yd_sum_loan_amt
    FROM dwd.dwd_loan_detail src
    WHERE src.stat_date = DATE_ADD('${hivevar:stat_date}', -1)
    GROUP BY src.product_code
)

INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily
PARTITION (stat_date)
SELECT
    -- ===== 维度字段 =====
    a.product_code,                                              -- 产品编码
    dim_prod.product_name,                                       -- 产品名称

    -- ===== 指标字段 =====
    COALESCE(a.td_sum_loan_amt, 0)       AS td_sum_loan_amt,    -- 当日放款总金额
    COALESCE(a.td_cnt_loan, 0)           AS td_cnt_loan,        -- 当日放款笔数
    COALESCE(a.td_sum_loan_amt, 0)
        - COALESCE(ap.yd_sum_loan_amt, 0)
                                         AS td_diff_loan_amt,   -- 日环比差值

    -- ===== 分区字段 =====
    '${hivevar:stat_date}'               AS stat_date

FROM agg a
-- 关联维度: 产品名称
LEFT JOIN dim.dim_product dim_prod
    ON a.product_code = dim_prod.product_code
-- 关联: 昨日数据（环比）
LEFT JOIN agg_prev ap
    ON a.product_code = ap.product_code
;
```

---

## 2. 初始化脚本示例

当使用 `--mode=init` 时，还会生成以下初始化脚本：

**生成脚本：** `dm/dmm_sac_loan_prod_daily_init.sql`

```sql
-- ============================================================
-- 脚本:    dm/dmm_sac_loan_prod_daily_init.sql
-- 功能:    贷款产品日维度指标宽表 - 历史数据初始化
-- 目标表:  dm.dmm_sac_loan_prod_daily
-- 源表:    dwd.dwd_loan_detail, dim.dim_product
-- 粒度:    一行 = 一天 × 一产品
-- 作者:    auto-generated
-- 创建日期: 2026-01-31
-- ============================================================
--
-- 使用场景: 新表上线时一次性回刷历史数据
--
-- 执行方式:
--   方式 1 (指定日期范围):
--     hive -hivevar start_date=2024-01-01 -hivevar end_date=2024-12-31 \
--          -f dmm_sac_loan_prod_daily_init.sql
--
--   方式 2 (使用 Shell 计算最近 N 天):
--     start_date=$(date -d "30 days ago" +%Y-%m-%d)
--     end_date=$(date -d "yesterday" +%Y-%m-%d)
--     hive -hivevar start_date=$start_date -hivevar end_date=$end_date \
--          -f dmm_sac_loan_prod_daily_init.sql
--
-- 注意事项:
--   1. 仅在新表上线或需要全量修复时执行
--   2. 大数据量时建议分批执行（如按月回刷）
--   3. 执行前确认目标表分区可覆盖
--   4. 日常增量调度使用 dmm_sac_loan_prod_daily_etl.sql
--
-- ============================================================

-- === 动态分区配置（初始化脚本必需）===
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.parallel=true;

-- === ETL 主逻辑 ===
WITH
-- CTE 1: 时间范围内放款聚合（按日期分组）
agg AS (
    SELECT
        src.stat_date,                           -- 新增：分区字段
        src.product_code,
        SUM(src.loan_amount)             AS td_sum_loan_amt,
        COUNT(src.loan_id)               AS td_cnt_loan
    FROM dwd.dwd_loan_detail src
    WHERE src.stat_date BETWEEN '${hivevar:start_date}' AND '${hivevar:end_date}'  -- 时间范围过滤
    GROUP BY src.stat_date, src.product_code   -- 新增：stat_date 分组
),
-- CTE 2: 计算每天的昨日数据（用于环比）
agg_with_prev AS (
    SELECT
        stat_date,
        product_code,
        td_sum_loan_amt,
        td_cnt_loan,
        LAG(td_sum_loan_amt, 1) OVER (PARTITION BY product_code ORDER BY stat_date) AS yd_sum_loan_amt
    FROM agg
)

INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily
PARTITION (stat_date)  -- 动态分区
SELECT
    -- ===== 维度字段 =====
    a.product_code,                                              -- 产品编码
    dim_prod.product_name,                                       -- 产品名称

    -- ===== 指标字段 =====
    COALESCE(a.td_sum_loan_amt, 0)       AS td_sum_loan_amt,    -- 当日放款总金额
    COALESCE(a.td_cnt_loan, 0)           AS td_cnt_loan,        -- 当日放款笔数
    COALESCE(a.td_sum_loan_amt, 0)
        - COALESCE(a.yd_sum_loan_amt, 0)
                                         AS td_diff_loan_amt,   -- 日环比差值

    -- ===== 分区字段 =====
    a.stat_date                          AS stat_date            -- 动态分区字段

FROM agg_with_prev a
-- 关联维度: 产品名称
LEFT JOIN dim.dim_product dim_prod
    ON a.product_code = dim_prod.product_code
;
```

---

## 3. 增量 vs 初始化关键差异

| 元素 | 增量脚本 | 初始化脚本 |
|------|---------|-----------|
| **分区写入** | `PARTITION (stat_date)` 静态 | `PARTITION (stat_date)` 动态 |
| **时间过滤** | `WHERE stat_date = '${stat_date}'` | `WHERE stat_date BETWEEN '${start_date}' AND '${end_date}'` |
| **GROUP BY** | `GROUP BY product_code` | `GROUP BY stat_date, product_code` |
| **环比计算** | LEFT JOIN 昨日聚合 | 使用 LAG 窗口函数（性能更优） |
| **SELECT 分区** | `'${hivevar:stat_date}' AS stat_date` 静态 | `a.stat_date AS stat_date` 动态来源 |
| **动态分区配置** | 不需要 | 必须开启 |
