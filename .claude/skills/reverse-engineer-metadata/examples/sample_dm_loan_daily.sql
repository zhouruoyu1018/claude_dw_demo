-- ==============================================================================
-- 脚本名称: dm_loan_daily.sql
-- 作者: legacy_dev
-- 创建日期: 2024-12-01
-- 描述: 按产品维度汇总每日放款数据
-- 修改记录:
--   2024-12-15: 新增逾期金额字段 (legacy_dev)
-- ==============================================================================

INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily PARTITION (dt='${hivevar:dt}')
SELECT
    -- 维度字段
    a.dt,
    a.product_id,
    b.product_name,
    b.product_type,

    -- 放款指标
    SUM(a.loan_amount) AS td_loan_amt,
    COUNT(DISTINCT a.loan_id) AS td_loan_cnt,
    SUM(CASE WHEN a.loan_status = 'SUCCESS' THEN 1 ELSE 0 END) AS td_success_cnt,
    SUM(CASE WHEN a.loan_status = 'FAILED' THEN 1 ELSE 0 END) AS td_failed_cnt,

    -- 派生指标
    SUM(a.loan_amount) / NULLIF(COUNT(DISTINCT a.loan_id), 0) AS avg_loan_amt,
    SUM(CASE WHEN a.loan_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT a.loan_id), 0) AS success_rate,

    -- 逾期指标（关联 dws 层）
    COALESCE(c.overdue_amt, 0) AS td_overdue_amt,
    COALESCE(c.overdue_cnt, 0) AS td_overdue_cnt

FROM dwd.dwd_loan_detail a

-- 关联产品主数据
LEFT JOIN dwd.dwd_product_info b
    ON a.product_id = b.product_id

-- 关联逾期汇总数据
LEFT JOIN dws.dws_overdue_summary c
    ON a.product_id = c.product_id
    AND a.dt = c.dt

WHERE a.dt = '${hivevar:dt}'
    AND a.is_deleted = 0

GROUP BY
    a.dt,
    a.product_id,
    b.product_name,
    b.product_type,
    c.overdue_amt,  -- 需要包含在 GROUP BY 中（因为是直接引用）
    c.overdue_cnt
;
