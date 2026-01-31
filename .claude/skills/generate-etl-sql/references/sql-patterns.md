# 复杂 SQL 模式速查

面向 dm/da 层 ETL 开发的常用复杂 SQL 模式。

---

## 1. Window Functions（窗口函数）

### 1.1 环比 / 同比

```sql
-- 日环比：当日 vs 昨日
LAG(td_sum_loan_amt, 1) OVER (
    PARTITION BY product_code
    ORDER BY dt
) AS yd_sum_loan_amt

-- 周同比：当日 vs 上周同天
LAG(td_sum_loan_amt, 7) OVER (
    PARTITION BY product_code
    ORDER BY dt
) AS lw_sum_loan_amt

-- 月同比：当月 vs 上月同期
LAG(mon_sum_loan_amt, 1) OVER (
    PARTITION BY product_code
    ORDER BY month_id
) AS lm_sum_loan_amt

-- 环比增长率
(td_sum_loan_amt - LAG(td_sum_loan_amt, 1) OVER (...))
    / NULLIF(LAG(td_sum_loan_amt, 1) OVER (...), 0)
    AS td_growth_rate
```

### 1.2 排名

```sql
-- Top N 客户
ROW_NUMBER() OVER (
    PARTITION BY dt
    ORDER BY loan_amount DESC
) AS rn

-- 并列排名（跳号）
RANK() OVER (
    PARTITION BY product_code
    ORDER BY overdue_days DESC
) AS rank_overdue

-- 并列排名（不跳号）
DENSE_RANK() OVER (
    PARTITION BY channel_code
    ORDER BY td_sum_loan_amt DESC
) AS drank
```

### 1.3 累计 / 移动窗口

```sql
-- 月初至今累计
SUM(td_sum_loan_amt) OVER (
    PARTITION BY product_code, SUBSTR(dt, 1, 7)
    ORDER BY dt
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS mtd_sum_loan_amt

-- 年初至今累计
SUM(td_sum_loan_amt) OVER (
    PARTITION BY product_code, SUBSTR(dt, 1, 4)
    ORDER BY dt
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS ytd_sum_loan_amt

-- 近7日移动平均
AVG(td_sum_loan_amt) OVER (
    PARTITION BY product_code
    ORDER BY dt
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS p7d_avg_loan_amt

-- 近30日滑动求和
SUM(td_sum_loan_amt) OVER (
    PARTITION BY product_code
    ORDER BY dt
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
) AS p30d_sum_loan_amt
```

### 1.4 首末值

```sql
-- 首次放款日期
FIRST_VALUE(loan_date) OVER (
    PARTITION BY cust_id
    ORDER BY loan_date
) AS first_loan_date

-- 最近一次还款日期
LAST_VALUE(repay_date) OVER (
    PARTITION BY loan_id
    ORDER BY repay_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS last_repay_date
```

> **注意：** `LAST_VALUE` 默认窗口帧是 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`，需要显式指定 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` 才能获取真正的末尾值。

---

## 2. 多表 JOIN 模式

### 2.1 星型关联（事实+维度）

```sql
FROM dwd.dwd_loan_detail src
-- 产品维度
LEFT JOIN dim.dim_product dim_prod
    ON src.product_code = dim_prod.product_code
-- 渠道维度
LEFT JOIN dim.dim_channel dim_chn
    ON src.channel_code = dim_chn.channel_code
-- 客户维度
LEFT JOIN dim.dim_customer dim_cust
    ON src.cust_id = dim_cust.cust_id
    AND dim_cust.dt = '${dt}'   -- 拉链表取当日快照
```

### 2.2 多事实表关联

```sql
WITH
loan_agg AS (
    SELECT product_code, SUM(loan_amount) AS sum_loan_amt
    FROM dwd.dwd_loan_detail WHERE dt = '${dt}'
    GROUP BY product_code
),
repay_agg AS (
    SELECT product_code, SUM(repay_amount) AS sum_repay_amt
    FROM dwd.dwd_repay_detail WHERE dt = '${dt}'
    GROUP BY product_code
),
overdue_agg AS (
    SELECT product_code, COUNT(loan_id) AS cnt_overdue
    FROM dwd.dwd_overdue_detail WHERE dt = '${dt}' AND overdue_days > 0
    GROUP BY product_code
)
SELECT
    COALESCE(la.product_code, ra.product_code, oa.product_code) AS product_code,
    la.sum_loan_amt,
    ra.sum_repay_amt,
    oa.cnt_overdue
FROM loan_agg la
FULL OUTER JOIN repay_agg ra ON la.product_code = ra.product_code
FULL OUTER JOIN overdue_agg oa ON la.product_code = oa.product_code;
```

### 2.3 Semi Join（存在性判断）

```sql
-- 有逾期记录的客户（Hive/Impala）
LEFT SEMI JOIN dwd.dwd_overdue_detail od
    ON src.loan_id = od.loan_id AND od.overdue_days > 0

-- 标准 SQL 写法（全引擎兼容）
WHERE EXISTS (
    SELECT 1 FROM dwd.dwd_overdue_detail od
    WHERE od.loan_id = src.loan_id AND od.overdue_days > 0
)

-- Anti Join: 无逾期记录的客户
WHERE NOT EXISTS (
    SELECT 1 FROM dwd.dwd_overdue_detail od
    WHERE od.loan_id = src.loan_id AND od.overdue_days > 0
)
```

### 2.4 自关联（历史对比）

```sql
-- 当日 vs 昨日同维度对比
FROM target_table cur
LEFT JOIN target_table prev
    ON cur.product_code = prev.product_code
    AND prev.dt = DATE_ADD(cur.dt, -1)
WHERE cur.dt = '${dt}'
```

---

## 3. Grouping Sets / CUBE / ROLLUP

### 3.1 GROUPING SETS（指定组合）

```sql
SELECT
    COALESCE(product_code, '全部')       AS product_code,
    COALESCE(channel_code, '全部')       AS channel_code,
    SUM(loan_amount)                     AS td_sum_loan_amt,
    GROUPING__ID                         AS grouping_id     -- Hive
FROM dwd.dwd_loan_detail
WHERE dt = '${dt}'
GROUP BY product_code, channel_code
GROUPING SETS (
    (product_code, channel_code),   -- 产品 × 渠道
    (product_code),                 -- 产品小计
    (channel_code),                 -- 渠道小计
    ()                              -- 总计
);
```

### 3.2 CUBE（全组合）

```sql
-- 等价于所有 2^N 种组合
GROUP BY product_code, channel_code
WITH CUBE
-- 等效于:
-- GROUPING SETS (
--     (product_code, channel_code),
--     (product_code),
--     (channel_code),
--     ()
-- )
```

### 3.3 ROLLUP（层级汇总）

```sql
-- 按层级从细到粗汇总
GROUP BY product_code, channel_code
WITH ROLLUP
-- 等效于:
-- GROUPING SETS (
--     (product_code, channel_code),
--     (product_code),
--     ()
-- )
-- 注意: 不含 (channel_code) 单独分组
```

### 3.4 识别汇总行

```sql
-- Hive: GROUPING__ID (双下划线，位图)
CASE GROUPING__ID
    WHEN 0 THEN '明细'      -- product + channel 均有值
    WHEN 1 THEN '产品小计'   -- channel 为 NULL
    WHEN 2 THEN '渠道小计'   -- product 为 NULL
    WHEN 3 THEN '总计'       -- 均为 NULL
END AS level_desc

-- Impala/Doris: GROUPING() 函数
CASE
    WHEN GROUPING(product_code) = 0 AND GROUPING(channel_code) = 0 THEN '明细'
    WHEN GROUPING(product_code) = 0 AND GROUPING(channel_code) = 1 THEN '产品小计'
    WHEN GROUPING(product_code) = 1 AND GROUPING(channel_code) = 0 THEN '渠道小计'
    ELSE '总计'
END AS level_desc
```

---

## 4. 增量加载模式

### 4.1 分区增量（最常见）

```sql
-- 每日仅加工当日分区，覆写目标表当日分区
INSERT OVERWRITE TABLE dm.target_table
PARTITION (dt)
SELECT ...
FROM dwd.source_table
WHERE dt = '${dt}'   -- 仅取当日数据
GROUP BY ...
;
```

### 4.2 多日回溯

```sql
-- 回溯N天数据（逾期场景：逾期状态可能延迟更新）
INSERT OVERWRITE TABLE dm.target_table
PARTITION (dt)
SELECT ...
FROM dwd.source_table
WHERE dt BETWEEN DATE_ADD('${dt}', -7) AND '${dt}'
GROUP BY ..., dt
;
```

### 4.3 全量快照

```sql
-- 每日全量重算（维度变化或口径变更时使用）
INSERT OVERWRITE TABLE dm.target_table
PARTITION (dt = '${dt}')
SELECT ...
FROM dwd.source_table src
-- 取所有历史数据
WHERE src.dt <= '${dt}'
GROUP BY ...
;
```

---

## 5. 数据清洗模式

### 5.1 去重

```sql
-- 按主键取最新记录
WITH dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id
            ORDER BY update_time DESC
        ) AS rn
    FROM ods.source_table
    WHERE dt = '${dt}'
)
SELECT * FROM dedup WHERE rn = 1;
```

### 5.2 行列转换

```sql
-- 行转列（Hive）
SELECT
    loan_id,
    MAX(CASE WHEN period = 1 THEN repay_amount END)  AS period_1_amt,
    MAX(CASE WHEN period = 2 THEN repay_amount END)  AS period_2_amt,
    MAX(CASE WHEN period = 3 THEN repay_amount END)  AS period_3_amt
FROM dwd.dwd_repay_schedule
GROUP BY loan_id;

-- 列转行（Hive: LATERAL VIEW EXPLODE）
SELECT loan_id, period_num, period_amt
FROM source_table
LATERAL VIEW POSEXPLODE(ARRAY(period_1_amt, period_2_amt, period_3_amt))
    t AS period_num, period_amt;
```

### 5.3 NULL 处理

```sql
-- 数值型默认值
COALESCE(loan_amount, 0)                AS loan_amount
-- 字符型默认值
COALESCE(product_name, '未知')           AS product_name
-- 除零保护
CASE
    WHEN denominator = 0 THEN NULL
    ELSE numerator / denominator
END                                      AS ratio
-- 或
numerator / NULLIF(denominator, 0)       AS ratio
```
