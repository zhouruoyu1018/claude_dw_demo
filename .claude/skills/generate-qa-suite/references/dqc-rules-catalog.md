# DQC 规则完整目录

数据质量检查规则分为六大类，每条规则包含编号、适用场景、默认阈值和 SQL 模板。

---

## 规则编号体系

```
DQC-{类别缩写}{序号}

C  = Completeness  (完整性)
U  = Uniqueness    (唯一性)
V  = Validity      (有效性)
CS = Consistency   (一致性)
VOL = Volatility   (波动性)
T  = Timeliness    (时效性)
```

---

## 1. 完整性规则 (Completeness)

### DQC-C01: 表非空

| 属性 | 值 |
|------|-----|
| 适用 | 所有目标表 |
| 级别 | FATAL |
| 阈值 | COUNT(*) > 0 |

```sql
SELECT COUNT(*) AS cnt
FROM {table} WHERE dt = '${dt}';
-- FATAL if cnt = 0
```

### DQC-C02: 字段非 NULL

| 属性 | 值 |
|------|-----|
| 适用 | 主键字段、维度编码字段 |
| 级别 | ERROR |
| 阈值 | NULL 比例 = 0% |

```sql
SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_cnt,
       COUNT(*) AS total_cnt,
       ROUND(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS null_rate
FROM {table} WHERE dt = '${dt}';
-- ERROR if null_cnt > 0
```

### DQC-C03: 字段非空串

| 属性 | 值 |
|------|-----|
| 适用 | STRING 类型维度字段 |
| 级别 | WARN |
| 阈值 | 空串比例 = 0% |

```sql
SELECT SUM(CASE WHEN TRIM({col}) = '' THEN 1 ELSE 0 END) AS empty_cnt
FROM {table} WHERE dt = '${dt}';
-- WARN if empty_cnt > 0
```

### DQC-C04: 分区完整性

| 属性 | 值 |
|------|-----|
| 适用 | 日分区表 |
| 级别 | FATAL |
| 阈值 | 当日分区存在 |

```sql
SELECT COUNT(DISTINCT dt) AS partition_cnt
FROM {table}
WHERE dt = '${dt}';
-- FATAL if partition_cnt = 0
```

---

## 2. 唯一性规则 (Uniqueness)

### DQC-U01: 主键唯一

| 属性 | 值 |
|------|-----|
| 适用 | 所有目标表（基于 logical_primary_key） |
| 级别 | FATAL |
| 阈值 | 重复组数 = 0 |

```sql
SELECT COUNT(*) AS dup_group_cnt
FROM (
    SELECT {pk_cols}, COUNT(*) AS cnt
    FROM {table} WHERE dt = '${dt}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) t;
-- FATAL if dup_group_cnt > 0
```

### DQC-U02: 重复行（全字段）

| 属性 | 值 |
|------|-----|
| 适用 | 无明确主键的表 |
| 级别 | WARN |
| 阈值 | 重复率 < 1% |

```sql
SELECT total_cnt - distinct_cnt AS dup_cnt,
       ROUND((total_cnt - distinct_cnt) / NULLIF(total_cnt, 0), 4) AS dup_rate
FROM (
    SELECT COUNT(*) AS total_cnt,
           COUNT(DISTINCT {all_cols_concat}) AS distinct_cnt
    FROM {table} WHERE dt = '${dt}'
) t;
-- WARN if dup_rate > 0.01
```

---

## 3. 有效性规则 (Validity)

### DQC-V01: 金额非负

| 属性 | 值 |
|------|-----|
| 适用 | DECIMAL 类型 + 字段名含 `_amt` / `_bal` / `_prin` / `_int` / `_fee` |
| 级别 | ERROR |
| 阈值 | 负值行数 = 0 |

```sql
SELECT SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END) AS negative_cnt
FROM {table} WHERE dt = '${dt}';
-- ERROR if negative_cnt > 0
```

### DQC-V02: 比率范围

| 属性 | 值 |
|------|-----|
| 适用 | DECIMAL(10,4) + 字段名含 `rat_` / `_rate` |
| 级别 | ERROR |
| 阈值 | 全部落在 [0, 1]（可配置为 [0, 100] 百分比场景） |

```sql
SELECT SUM(CASE WHEN {col} < 0 OR {col} > 1 THEN 1 ELSE 0 END) AS out_cnt
FROM {table} WHERE dt = '${dt}';
-- ERROR if out_cnt > 0
```

### DQC-V03: 布尔枚举

| 属性 | 值 |
|------|-----|
| 适用 | TINYINT + 字段名以 `is_` / `has_` 开头 |
| 级别 | ERROR |
| 阈值 | 值域 ⊂ {0, 1} |

```sql
SELECT SUM(CASE WHEN {col} NOT IN (0, 1) THEN 1 ELSE 0 END) AS invalid_cnt
FROM {table} WHERE dt = '${dt}';
-- ERROR if invalid_cnt > 0
```

### DQC-V04: 计数非负

| 属性 | 值 |
|------|-----|
| 适用 | BIGINT/INT + 字段名含 `_cnt` / `_num` |
| 级别 | ERROR |
| 阈值 | 负值行数 = 0 |

```sql
SELECT SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END) AS negative_cnt
FROM {table} WHERE dt = '${dt}';
-- ERROR if negative_cnt > 0
```

### DQC-V05: 日期格式

| 属性 | 值 |
|------|-----|
| 适用 | STRING + 字段名含 `_date` |
| 级别 | ERROR |
| 阈值 | 非法格式行数 = 0 |

```sql
-- Hive
SELECT SUM(CASE WHEN {col} NOT RLIKE '^\\d{4}-\\d{2}-\\d{2}$' THEN 1 ELSE 0 END) AS bad_cnt
FROM {table} WHERE dt = '${dt}';

-- Impala / Doris
SELECT SUM(CASE WHEN {col} NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END) AS bad_cnt
FROM {table} WHERE dt = '${dt}';
```

### DQC-V06: 天数合理范围

| 属性 | 值 |
|------|-----|
| 适用 | INT + 字段名含 `_days` |
| 级别 | WARN |
| 阈值 | [0, 3650]（10年） |

```sql
SELECT SUM(CASE WHEN {col} < 0 OR {col} > 3650 THEN 1 ELSE 0 END) AS out_cnt
FROM {table} WHERE dt = '${dt}';
-- WARN if out_cnt > 0
```

### DQC-V07: 枚举值白名单

| 属性 | 值 |
|------|-----|
| 适用 | 状态字段（COMMENT 中列举了枚举值） |
| 级别 | WARN |
| 阈值 | 非法枚举行数 = 0 |

```sql
-- 由 COMMENT 中提取枚举值，如 "还款状态，1-正常 2-逾期 3-核销"
SELECT SUM(CASE WHEN {col} NOT IN ({enum_values}) THEN 1 ELSE 0 END) AS invalid_cnt
FROM {table} WHERE dt = '${dt}';
```

---

## 4. 一致性规则 (Consistency)

### DQC-CS01: 引用完整性

| 属性 | 值 |
|------|-----|
| 适用 | 维度编码 `_code` 字段 |
| 级别 | WARN |
| 阈值 | 孤儿记录数 = 0 |

```sql
SELECT COUNT(*) AS orphan_cnt
FROM {table} t
LEFT JOIN {dim_table} d ON t.{code_col} = d.{code_col}
WHERE t.dt = '${dt}' AND d.{code_col} IS NULL AND t.{code_col} IS NOT NULL;
-- WARN if orphan_cnt > 0
```

### DQC-CS02: 跨层汇总一致

| 属性 | 值 |
|------|-----|
| 适用 | dm/da 层金额指标 vs dwd 层明细 |
| 级别 | ERROR |
| 阈值 | 差异 < 0.01（分以内） |

```sql
SELECT ABS(
    (SELECT SUM({tgt_amt}) FROM {target_table} WHERE dt = '${dt}')
  - (SELECT SUM({src_amt}) FROM {source_table} WHERE dt = '${dt}')
) AS diff;
-- ERROR if diff >= 0.01
```

### DQC-CS03: 逻辑一致性

| 属性 | 值 |
|------|-----|
| 适用 | 存在推导关系的字段 |
| 级别 | ERROR |
| 阈值 | 不一致行数 = 0 |

```sql
-- 示例: 计数 >= 0 时金额应 >= 0
SELECT COUNT(*) AS inconsistent_cnt
FROM {table}
WHERE dt = '${dt}'
  AND td_cnt_loan > 0
  AND td_sum_loan_amt <= 0;
-- ERROR if inconsistent_cnt > 0

-- 示例: 比率 = 分子 / 分母
SELECT COUNT(*) AS inconsistent_cnt
FROM {table}
WHERE dt = '${dt}'
  AND denominator > 0
  AND ABS(ratio - numerator / denominator) > 0.0001;
```

---

## 5. 波动性规则 (Volatility)

### DQC-VOL01: 行数波动

| 属性 | 值 |
|------|-----|
| 适用 | 所有目标表 |
| 级别 | WARN |
| 阈值 | 波动率 < 50%（可配置） |

```sql
SELECT ABS(today_cnt - yesterday_cnt) / NULLIF(yesterday_cnt, 0) AS volatility
FROM (
    SELECT
        (SELECT COUNT(*) FROM {table} WHERE dt = '${dt}') AS today_cnt,
        (SELECT COUNT(*) FROM {table} WHERE dt = DATE_ADD('${dt}', -1)) AS yesterday_cnt
) t;
-- WARN if volatility > 0.5
```

### DQC-VOL02: 指标波动

| 属性 | 值 |
|------|-----|
| 适用 | 核心金额/计数指标 |
| 级别 | WARN |
| 阈值 | 波动率 < 100%（可配置） |

```sql
SELECT ABS(today_val - yesterday_val) / NULLIF(yesterday_val, 0) AS volatility
FROM (
    SELECT
        (SELECT SUM({metric}) FROM {table} WHERE dt = '${dt}') AS today_val,
        (SELECT SUM({metric}) FROM {table} WHERE dt = DATE_ADD('${dt}', -1)) AS yesterday_val
) t;
-- WARN if volatility > 1.0
```

### DQC-VOL03: 趋势异常（7日均线偏离）

| 属性 | 值 |
|------|-----|
| 适用 | 核心指标（可选启用） |
| 级别 | WARN |
| 阈值 | 当日值偏离 7 日均线超 3 倍标准差 |

```sql
WITH daily AS (
    SELECT dt, SUM({metric}) AS val
    FROM {table}
    WHERE dt BETWEEN DATE_ADD('${dt}', -7) AND '${dt}'
    GROUP BY dt
),
stats AS (
    SELECT AVG(val) AS avg_val, STDDEV(val) AS std_val
    FROM daily WHERE dt < '${dt}'  -- 排除当日
)
SELECT d.val AS today_val, s.avg_val, s.std_val,
       ABS(d.val - s.avg_val) / NULLIF(s.std_val, 0) AS z_score,
       CASE WHEN ABS(d.val - s.avg_val) / NULLIF(s.std_val, 0) > 3 THEN 'WARN' ELSE 'PASS' END AS result
FROM daily d, stats s
WHERE d.dt = '${dt}';
-- WARN if z_score > 3
```

---

## 6. 时效性规则 (Timeliness)

### DQC-T01: 分区新鲜度

| 属性 | 值 |
|------|-----|
| 适用 | 每日调度表 |
| 级别 | FATAL |
| 阈值 | 最新分区 = 当日 |

```sql
SELECT MAX(dt) AS latest_dt,
       DATEDIFF('${dt}', MAX(dt)) AS lag_days,
       CASE WHEN MAX(dt) = '${dt}' THEN 'PASS' ELSE 'FATAL' END AS result
FROM {table};
-- FATAL if latest_dt != ${dt}
```

### DQC-T02: 上游依赖时效

| 属性 | 值 |
|------|-----|
| 适用 | ETL 脚本依赖的源表 |
| 级别 | FATAL |
| 阈值 | 所有源表当日分区已就绪 |

```sql
-- 对每张源表检查
SELECT '{source_table}' AS table_name,
       COUNT(*) AS cnt,
       CASE WHEN COUNT(*) > 0 THEN 'READY' ELSE 'NOT_READY' END AS status
FROM {source_table}
WHERE dt = '${dt}';
```

---

## 阈值配置参考

| 规则 | 默认阈值 | 可调范围 | 说明 |
|------|---------|---------|------|
| 行数波动 | 50% | 10% ~ 200% | 业务高峰期可放宽 |
| 指标波动 | 100% | 30% ~ 500% | 新产品上线期放宽 |
| 比率范围 | [0, 1] | [0, 100] | 百分比存储时调整 |
| 天数上限 | 3650 | 按业务调整 | 贷款最长期限 |
| 跨层差异 | 0.01 | 0 ~ 1 | 精度要求高时收紧 |
| Z-Score | 3 | 2 ~ 4 | 灵敏度调节 |
