# 命名规范与词根查询指南

## 1. 词根表 (word_root_dict)

所有字段命名必须基于词根表，查询 MySQL 库 `public.word_root_dict`。

### 1.1 查询方式

```sql
-- 按中文业务含义搜索
SELECT word_root, word_cn, word_en, category
FROM public.word_root_dict
WHERE word_cn LIKE '%放款%';

-- 按英文缩写搜索
SELECT word_root, word_cn, word_en, category
FROM public.word_root_dict
WHERE word_root LIKE '%loan%';

-- 按分类浏览
SELECT word_root, word_cn, word_en
FROM public.word_root_dict
WHERE category = '贷款'
ORDER BY word_root;
```

### 1.2 常用词根速查

以下为贷款业务常用词根，实际使用时仍需查表确认：

| 词根 | 中文 | 分类 |
|------|------|------|
| `apply` | 进件/申请 | 贷款销售 |
| `credit` | 授信 | 贷款销售 |
| `sign` | 签约 | 贷款销售 |
| `loan` | 放款/贷款 | 贷款销售 |
| `disburse` | 放款(出账) | 贷款销售 |
| `repay` | 还款 | 贷后管理 |
| `overdue` | 逾期 | 贷后管理 |
| `collect` | 催收 | 贷后管理 |
| `writeoff` | 核销 | 贷后管理 |
| `amt` | 金额 | 度量 |
| `prin` | 本金 | 度量 |
| `int` | 利息 | 度量 |
| `fee` | 费用 | 度量 |
| `bal` | 余额 | 度量 |
| `cnt` | 件数/笔数 | 度量 |
| `rat` | 比率 | 度量 |
| `days` | 天数 | 度量 |
| `cust` | 客户 | 实体 |
| `prod` | 产品 | 实体 |
| `chn` | 渠道 | 实体 |
| `org` | 机构 | 实体 |
| `acct` | 账户 | 实体 |

### 1.3 词根缺失处理

当词根表中没有匹配项时：
1. 使用通用英文缩写
2. 在字段 COMMENT 中标注 `（新词根，待入库）`
3. 告知用户需要向词根表新增该词根

---

## 2. 表命名规则

### 2.1 格式

```
{分层前缀}_{业务主题}_{数据粒度}[_{补充说明}]
```

### 2.2 分层前缀

| 分层 | 前缀 | 说明 |
|------|------|------|
| dm (数据集市) | `dmm_sac_` | 业务指标宽表，可复用 |
| da (数据应用) | `da_sac_` | 面向特定报表/接口 |

### 2.3 业务主题词

| 业务域 | 主题词 | 示例表名 |
|--------|--------|---------|
| 进件 | `apply` | `dmm_sac_apply_daily` |
| 授信 | `credit` | `dmm_sac_credit_chn_daily` |
| 签约 | `sign` | `dmm_sac_sign_prod_monthly` |
| 放款 | `loan` | `dmm_sac_loan_prod_daily` |
| 还款 | `repay` | `dmm_sac_repay_daily` |
| 逾期 | `overdue` | `dmm_sac_overdue_prod_daily` |
| 催收 | `collect` | `dmm_sac_collect_org_daily` |
| 核销 | `writeoff` | `dmm_sac_writeoff_monthly` |

### 2.4 粒度后缀

| 粒度 | 后缀 | 含义 |
|------|------|------|
| 日 | `_daily` | 按天汇总 |
| 周 | `_weekly` | 按周汇总 |
| 月 | `_monthly` | 按月汇总 |
| 明细 | `_dtl` | 明细级别 |

可在粒度前插入维度缩写：`_prod_daily`(产品+日), `_chn_monthly`(渠道+月)

---

## 3. 字段命名规则

### 3.1 命名组装顺序

```
{布尔前缀}_{时间范围}_{聚合方式}_{业务主题}_{分类词}
```

每段均可省略，但出现时必须按此顺序。

### 3.2 各段详解

#### 布尔前缀（第1段）

仅布尔字段使用：

| 前缀 | 含义 | 示例 |
|------|------|------|
| `is_` | 是否 | `is_first_loan` (是否首贷) |
| `has_` | 是否有 | `has_overdue_record` (是否有逾期记录) |

#### 时间范围（第2段）

| 前缀 | 含义 | 示例 |
|------|------|------|
| `td_` | 当日 (today) | `td_sum_loan_amt` |
| `yd_` | 昨日 (yesterday) | `yd_sum_repay_amt` |
| `cur_wk_` | 当周 | `cur_wk_cnt_apply` |
| `cur_mon_` | 当月 | `cur_mon_sum_loan_amt` |
| `cur_qtr_` | 当季 | `cur_qtr_sum_loan_amt` |
| `cur_yr_` | 当年 | `cur_yr_sum_loan_amt` |
| `his_` | 历史累计 | `his_max_overdue_days` |
| `lst_` | 最近一次 | `lst_repay_date` |
| `p{N}d_` | 过去N天 | `p30d_cnt_repay` |
| `p{N}m_` | 过去N月 | `p3m_avg_loan_amt` |

#### 聚合方式（第3段）

| 前缀 | 含义 | 示例 |
|------|------|------|
| `sum_` | 汇总 | `sum_loan_amt` |
| `cnt_` | 计数 | `cnt_loan` |
| `avg_` | 平均 | `avg_credit_amt` |
| `max_` | 最大 | `max_overdue_days` |
| `min_` | 最小 | `min_repay_amt` |
| `rat_` | 比率 | `rat_overdue_m1` |
| `dist_cnt_` | 去重计数 | `dist_cnt_cust` |

#### 业务主题（第4段）

核心含义，从词根表获取。组合规则：`{动作/实体}_{度量}`

| 组合 | 含义 |
|------|------|
| `loan_amt` | 放款金额 |
| `repay_prin` | 还款本金 |
| `overdue_days` | 逾期天数 |
| `credit_amt` | 授信额度 |
| `apply` | 进件（作为计数对象时可不加度量） |

#### 分类词（第5段）

| 后缀 | 含义 | 示例 |
|------|------|------|
| `_normal` | 正常 | `cnt_repay_normal` |
| `_overdue` | 逾期 | `cnt_repay_overdue` |
| `_m1` / `_m2` / `_m3` | M1/M2/M3期 | `rat_overdue_m1` |
| `_first` | 首次 | `is_first_overdue` |
| `_new` | 新增 | `cnt_loan_new` |

### 3.3 维度字段命名

| 类型 | 命名规则 | 示例 |
|------|---------|------|
| 主键/ID | `{实体}_id` | `loan_id`, `cust_id` |
| 编码 | `{实体}_code` | `product_code`, `channel_code` |
| 名称 | `{实体}_name` | `product_name`, `channel_name` |
| 日期 | `{实体}_date` | `loan_date`, `apply_date` |
| 状态 | `{实体}_status` | `loan_status` |

---

## 4. 字段排序规范

DDL 中字段按以下分组排列，组内按字母序：

```
第1组: 维度字段
  ├── 主键/ID（_id 结尾）
  ├── 编码（_code 结尾）
  ├── 名称（_name 结尾）
  ├── 日期（_date 结尾）
  └── 状态（_status 结尾）

第2组: 布尔字段（is_ / has_ 开头）

第3组: 时间类指标（按时间窗口从小到大）
  ├── td_（当日）
  ├── cur_mon_（当月）
  ├── cur_yr_（当年）
  ├── p{N}d_ / p{N}m_（过去N天/月）
  └── his_（历史）

第4组: 无时间前缀的聚合指标
  ├── sum_
  ├── cnt_
  ├── avg_
  ├── max_
  ├── min_
  └── rat_

第5组: 其他业务字段
```

每组之间在 DDL 中用注释行分隔：
```sql
    -- ===== 维度字段 =====
    ...
    -- ===== 布尔字段 =====
    ...
    -- ===== 指标字段 =====
    ...
```
