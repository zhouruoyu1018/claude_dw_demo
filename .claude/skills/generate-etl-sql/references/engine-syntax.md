# Hive / Impala / Doris 语法差异与兼容性

生成 ETL SQL 前必须确认目标引擎，参照此文档适配语法。

---

## 1. 核心语法差异

### 1.1 INSERT 语句

| 操作 | Hive | Impala | Doris |
|------|------|--------|-------|
| 分区覆写 | `INSERT OVERWRITE TABLE t PARTITION (stat_date)` | `INSERT OVERWRITE t PARTITION (stat_date)` | 不支持 OVERWRITE，用 Unique Model 自动 Upsert |
| 静态分区 | `PARTITION (stat_date = '2026-01-01')` | `PARTITION (stat_date = '2026-01-01')` | 无分区概念，用 WHERE 条件 |
| 动态分区 | 需要 `SET hive.exec.dynamic.partition.mode=nonstrict` | 默认支持 | N/A |
| 追加写入 | `INSERT INTO TABLE t` | `INSERT INTO t` | `INSERT INTO t` |

### 1.2 日期函数

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 日期加减 | `DATE_ADD('2026-01-01', N)` | `DAYS_ADD('2026-01-01', N)` | `DATE_ADD('2026-01-01', INTERVAL N DAY)` |
| 日期减 | `DATE_SUB('2026-01-01', N)` | `DAYS_SUB('2026-01-01', N)` | `DATE_SUB('2026-01-01', INTERVAL N DAY)` |
| 日期差 | `DATEDIFF(d1, d2)` — 返回天数 | `DATEDIFF(d1, d2)` | `DATEDIFF(d1, d2)` |
| 当前日期 | `CURRENT_DATE` | `CURRENT_DATE()` | `CURDATE()` |
| 日期格式化 | `DATE_FORMAT(stat_date, 'yyyy-MM-dd')` | `FROM_TIMESTAMP(ts, 'yyyy-MM-dd')` | `DATE_FORMAT(partition_key, '%Y-%m-%d')` |
| 月初 | `TRUNC(stat_date, 'MM')` | `TRUNC(stat_date, 'MM')` | `DATE_TRUNC(partition_key, 'month')` 或 `DATE_FORMAT(partition_key, '%Y-%m-01')` |
| 年初 | `TRUNC(stat_date, 'YY')` | `TRUNC(stat_date, 'YY')` | `DATE_TRUNC(partition_key, 'year')` |
| 取年 | `YEAR(stat_date)` | `YEAR(stat_date)` | `YEAR(partition_key)` |
| 取月 | `MONTH(stat_date)` | `MONTH(stat_date)` | `MONTH(partition_key)` |
| 上月末 | `LAST_DAY(ADD_MONTHS(stat_date, -1))` | `LAST_DAY(MONTHS_ADD(stat_date, -1))` | `LAST_DAY(DATE_SUB(partition_key, INTERVAL 1 MONTH))` |

### 1.3 字符串函数

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 拼接 | `CONCAT(a, b)` | `CONCAT(a, b)` 或 `a \|\| b` | `CONCAT(a, b)` |
| 截取 | `SUBSTR(s, pos, len)` | `SUBSTR(s, pos, len)` | `SUBSTRING(s, pos, len)` |
| 长度 | `LENGTH(s)` | `LENGTH(s)` | `LENGTH(s)` / `CHAR_LENGTH(s)` |
| 替换 | `REGEXP_REPLACE(s, pattern, rep)` | `REGEXP_REPLACE(s, pattern, rep)` | `REPLACE(s, old, new)` 或 `REGEXP_REPLACE` |
| 分割 | `SPLIT(s, sep)` | `SPLIT_PART(s, sep, idx)` | `SPLIT_PART(s, sep, idx)` |
| 去空格 | `TRIM(s)` | `TRIM(s)` | `TRIM(s)` |

### 1.4 NULL 处理

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 空值替换 | `NVL(x, default)` 或 `COALESCE` | `IFNULL(x, default)` 或 `COALESCE` | `IFNULL(x, default)` 或 `COALESCE` |
| 条件空值 | `NULLIF(a, b)` | `NULLIF(a, b)` | `NULLIF(a, b)` |
| 空值判断 | `x IS NULL` | `x IS NULL` | `x IS NULL` |

**推荐：** 统一使用 `COALESCE()`，三个引擎均支持。

### 1.5 类型转换

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 通用转换 | `CAST(x AS TYPE)` | `CAST(x AS TYPE)` | `CAST(x AS TYPE)` |
| 字符串→日期 | `TO_DATE(s)` | `TO_DATE(s, 'yyyy-MM-dd')` | `CAST(s AS DATE)` |
| 数字→字符串 | `CAST(n AS STRING)` | `CAST(n AS STRING)` | `CAST(n AS VARCHAR)` |

### 1.6 条件表达式

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| CASE | 支持 | 支持 | 支持 |
| IF | `IF(cond, v1, v2)` | `IF(cond, v1, v2)` | `IF(cond, v1, v2)` |
| DECODE | 不支持 | `DECODE(expr, v1, r1, ...)` | `CASE WHEN` 替代 |

---

## 2. Grouping Sets 差异

| 特性 | Hive | Impala | Doris |
|------|------|--------|-------|
| GROUPING SETS | 支持 | 支持 | 支持 |
| CUBE | `WITH CUBE` | `WITH CUBE` | `CUBE(...)` |
| ROLLUP | `WITH ROLLUP` | `WITH ROLLUP` | `ROLLUP(...)` |
| 标识汇总行 | `GROUPING__ID`（双下划线，位图值） | `GROUPING_ID(col1, col2, ...)` 函数 | `GROUPING_ID(col1, col2, ...)` 函数 |
| 单列标识 | `GROUPING(col)` — Hive 3.0+ | `GROUPING(col)` | `GROUPING(col)` |

**跨引擎写法：**

```sql
-- Hive
GROUPING__ID AS grp_id

-- Impala / Doris
GROUPING_ID(product_code, channel_code) AS grp_id
```

---

## 3. Window Functions 差异

三个引擎对窗口函数的支持基本一致，以下为少量差异：

| 特性 | Hive | Impala | Doris |
|------|------|--------|-------|
| ROW_NUMBER | 支持 | 支持 | 支持 |
| RANK / DENSE_RANK | 支持 | 支持 | 支持 |
| LAG / LEAD | 支持 | 支持 | 支持 |
| FIRST_VALUE / LAST_VALUE | 支持 | 支持 | 支持 |
| NTILE | 支持 | 支持 | 支持 |
| PERCENT_RANK | Hive 2.0+ | 支持 | 支持 |
| CUME_DIST | Hive 2.0+ | 支持 | 支持 |
| 窗口帧 ROWS | 支持 | 支持 | 支持 |
| 窗口帧 RANGE | 支持 | 部分支持 | 支持 |

---

## 4. JOIN 差异

| 特性 | Hive | Impala | Doris |
|------|------|--------|-------|
| INNER JOIN | 支持 | 支持 | 支持 |
| LEFT / RIGHT / FULL OUTER JOIN | 支持 | 支持 | 支持 |
| CROSS JOIN | 支持 | 支持 | 支持 |
| LEFT SEMI JOIN | 支持 | 支持 | 不支持，用 `EXISTS` |
| LEFT ANTI JOIN | 支持 | 支持 | 不支持，用 `NOT EXISTS` |
| MapJoin Hint | `/*+ MAPJOIN(t) */` | 自动优化 | `/*+ BROADCAST */` |
| Bucket Join | `/*+ STREAMTABLE(t) */` | N/A | Colocate Join |

**跨引擎兼容写法：**
```sql
-- 用 EXISTS 替代 SEMI JOIN（三引擎通用）
WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)

-- 用 NOT EXISTS 替代 ANTI JOIN（三引擎通用）
WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)
```

---

## 5. 参数化差异

| 引擎 | 变量写法 | 命令行注入 |
|------|---------|-----------|
| Hive | `${hivevar:stat_date}` | `hive -f script.sql -hivevar stat_date=2026-01-27` |
| Impala | `${var:stat_date}` | `impala-shell -f script.sql --var=stat_date=2026-01-27` |
| Doris | 无原生变量 | 应用层拼接 或 预处理脚本替换 `${partition_key}` |

**Doris 变通方案：**
```bash
# Shell 脚本预处理
PARTITION_KEY="2026-01-27"
sed "s/\${partition_key}/${PARTITION_KEY}/g" script.sql | mysql -h host -P 9030 -u user -p
```

---

## 6. 性能优化差异

### 6.1 Hive 优化参数

```sql
-- 并行执行
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;

-- MapJoin
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=50000000;  -- 50MB

-- 数据倾斜
SET hive.optimize.skewjoin=true;
SET hive.groupby.skewindata=true;

-- 动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;

-- 向量化
SET hive.vectorized.execution.enabled=true;

-- Tez 容器复用
SET tez.am.container.reuse.enabled=true;
```

### 6.2 Impala 优化参数

```sql
SET MEM_LIMIT=8g;
SET REQUEST_POOL='etl_pool';
SET NUM_SCANNER_THREADS=4;
-- 统计信息
COMPUTE STATS {table_name};
-- 或增量统计
COMPUTE INCREMENTAL STATS {table_name} PARTITION (stat_date = '${stat_date}');
```

### 6.3 Doris 优化

```sql
-- 并行度
SET parallel_fragment_exec_instance_num = 8;
-- 向量化
SET enable_vectorized_engine = true;
-- Profile 分析
SET is_report_success = true;
```

---

## 7. Doris 特殊语法

### 7.1 Stream Load（批量导入）

```bash
curl -u user:password \
  -H "label:load_${partition_key}" \
  -H "column_separator:," \
  -H "columns: col1, col2, col3" \
  -T data.csv \
  http://fe_host:8030/api/db/table/_stream_load
```

### 7.2 INSERT INTO ... SELECT（Doris 内部 ETL）

```sql
-- Doris Unique Model: 相同 Key 自动覆盖
INSERT INTO target_db.target_table
SELECT
    col1, col2, col3
FROM source_db.source_table
WHERE partition_key = '${partition_key}';
```

### 7.3 DELETE（Unique Model）

```sql
DELETE FROM target_db.target_table
WHERE partition_key = '${partition_key}' AND is_deleted = 1;
```

---

## 8. 快速选择指南

| 我需要... | 用哪个引擎 | 关键语法 |
|-----------|-----------|---------|
| T+1 批量 ETL | Hive | INSERT OVERWRITE + ORC + SNAPPY |
| 交互式查询验数 | Impala | INVALIDATE METADATA + COMPUTE STATS |
| 实时大屏写入 | Doris | INSERT INTO (Unique Model) |
| 多维度汇总报表 | Hive | GROUPING SETS + GROUPING__ID |
| 高并发查询服务 | Doris | Aggregate Model + 预聚合 |
| 快速原型验证 | Impala | 即时执行，秒级返回 |
