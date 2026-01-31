# Doris EXPLAIN 执行计划解读指南

Doris 查询上线前，通过 EXPLAIN 分析执行计划，识别性能瓶颈并优化。

---

## 1. EXPLAIN 命令

### 1.1 三种模式

```sql
-- 基础执行计划
EXPLAIN
SELECT ...;

-- 详细执行计划（含统计信息、资源估算）
EXPLAIN VERBOSE
SELECT ...;

-- 图形化执行计划
EXPLAIN GRAPH
SELECT ...;
```

### 1.2 输出结构

EXPLAIN 输出由多个 Fragment 组成，每个 Fragment 包含若干算子节点：

```
PLAN FRAGMENT 0           ← 最终结果返回给客户端
  └── RESULT SINK
      └── EXCHANGE NODE   ← 接收下层 Fragment 数据

PLAN FRAGMENT 1           ← 实际执行层
  └── DATA STREAM SINK    ← 向上层发送数据
      └── AGGREGATE NODE  ← 聚合计算
          └── HASH JOIN   ← JOIN 操作
              ├── EXCHANGE NODE (左)
              └── EXCHANGE NODE (右)

PLAN FRAGMENT 2           ← 数据扫描层
  └── DATA STREAM SINK
      └── OlapScanNode    ← 扫描存储层
```

---

## 2. 核心算子解读

### 2.1 OlapScanNode（数据扫描）

```
0:OlapScanNode
   TABLE: dm.dmm_sac_loan_prod_daily
   PREAGGREGATION: ON           ← 预聚合是否生效
   partitions=1/30              ← 分区裁剪: 扫描1个/共30个
   rollup: dmm_sac_loan_prod_daily   ← 命中的物化视图
   tabletRatio=10/10            ← 扫描 Tablet 数
   cardinality=50000            ← 估算行数
   avgRowSize=128               ← 平均行大小
   Predicates: dt = '2026-01-27'     ← 下推谓词
```

**检查要点：**

| 指标 | 正常 | 异常 | 优化方向 |
|------|------|------|---------|
| `partitions` | `1/N`（少量） | `N/N`（全扫描） | WHERE 条件是否含分区字段 |
| `PREAGGREGATION` | `ON` | `OFF` | Aggregate Model 预聚合未生效，检查 Key 列 |
| `rollup` | 物化视图名 | 基表名 | 创建物化视图或调整查询 |
| `cardinality` | 合理行数 | 极大值 | 谓词未下推或统计信息过期 |

### 2.2 HASH JOIN

```
3:HASH JOIN
   join op: LEFT OUTER JOIN
   equal join conjunct: t1.product_code = t2.product_code
   runtime filters: RF000[bloom] <- t2.product_code
   cardinality=50000
   hash-table-size=2MB          ← Hash 表大小
```

**检查要点：**

| 指标 | 正常 | 异常 | 优化方向 |
|------|------|------|---------|
| `hash-table-size` | < 内存限制 | 过大（>数GB） | 小表用 Broadcast，大表用 Shuffle |
| `runtime filters` | 存在 | 不存在 | 确认统计信息已更新 |
| Join 类型 | BROADCAST | SHUFFLE | 小表关联优先 Broadcast |

### 2.3 AGGREGATE

```
4:AGGREGATE (update finalize)
   output: SUM(loan_amount), COUNT(loan_id)
   group by: product_code
   cardinality=100

   -- 或两阶段聚合
4:AGGREGATE (merge finalize)     ← 第二阶段: 合并
   └── 5:AGGREGATE (update serialize)  ← 第一阶段: 局部聚合
```

**检查要点：**

| 指标 | 正常 | 异常 | 优化方向 |
|------|------|------|---------|
| 聚合方式 | 两阶段 | 单阶段 | 检查数据分布 |
| `cardinality` | 大幅减少 | 与输入接近 | GROUP BY 基数过高 |

### 2.4 EXCHANGE

```
6:EXCHANGE NODE
   type: SHUFFLE                ← 数据分发方式
   partition: HASH(product_code)
```

**分发类型：**

| 类型 | 含义 | 场景 |
|------|------|------|
| `UNPARTITIONED` | 汇聚到单节点 | 最终结果返回 |
| `SHUFFLE` | 按 Hash 重分布 | 大表 JOIN / GROUP BY |
| `BROADCAST` | 复制到所有节点 | 小表广播 |

---

## 3. 性能瓶颈识别清单

### 3.1 生成的检查项

对每条 ETL SQL，自动生成以下 EXPLAIN 检查项：

```sql
-- ============================================================
-- 性能分析: {target_table}
-- 引擎: Doris
-- ============================================================

-- [PERF-01] 执行计划概览
EXPLAIN {etl_select_query};

-- [PERF-02] 详细执行计划
EXPLAIN VERBOSE {etl_select_query};
```

配合以下检查清单：

```
□ 分区裁剪: OlapScanNode.partitions 是否为 1/N（非 N/N）
□ 预聚合:   PREAGGREGATION 是否为 ON
□ 物化视图: rollup 是否命中预期的物化视图
□ JOIN 类型: 小表是否 BROADCAST，大表是否 SHUFFLE
□ Hash 表:  hash-table-size 是否在内存限制内
□ 谓词下推: Predicates 是否包含过滤条件
□ 聚合阶段: 是否为两阶段聚合
□ 数据倾斜: cardinality 在各 Fragment 间是否均衡
```

### 3.2 常见问题与优化

| 问题 | 现象 | 优化 SQL/操作 |
|------|------|--------------|
| 分区未裁剪 | `partitions=N/N` | 确认 WHERE 含分区字段 |
| 预聚合失效 | `PREAGGREGATION: OFF` | 检查查询是否包含非 Key 列的非聚合引用 |
| 物化视图未命中 | `rollup: base_table` | 创建匹配的物化视图 |
| Shuffle 过多 | 大量 EXCHANGE SHUFFLE | 使用 Colocate Join |
| Hash 表过大 | `hash-table-size > 2GB` | 增大 `exec_mem_limit` 或优化 JOIN 顺序 |
| 统计信息过期 | `cardinality` 与实际偏差大 | `ANALYZE TABLE {table}` |

---

## 4. 优化建议自动生成

根据 EXPLAIN 结果，自动给出优化建议模板：

### 4.1 Colocate Join（消除 Shuffle）

```sql
-- 当两张表频繁 JOIN 且分桶列相同时
-- 建表时指定 Colocate Group
CREATE TABLE t1 (...)
DISTRIBUTED BY HASH(product_code) BUCKETS 10
PROPERTIES ("colocate_with" = "loan_group");

CREATE TABLE t2 (...)
DISTRIBUTED BY HASH(product_code) BUCKETS 10
PROPERTIES ("colocate_with" = "loan_group");
```

### 4.2 物化视图

```sql
-- 当查询频繁聚合某些维度+指标时
CREATE MATERIALIZED VIEW mv_loan_daily AS
SELECT
    dt,
    product_code,
    SUM(loan_amount) AS sum_loan_amt,
    COUNT(loan_id) AS cnt_loan
FROM dm.dmm_sac_loan_prod_daily
GROUP BY dt, product_code;
```

### 4.3 统计信息更新

```sql
-- 更新表统计信息（影响执行计划选择）
ANALYZE TABLE {table};

-- 仅更新指定列
ANALYZE TABLE {table} ({col1}, {col2});
```

### 4.4 查询 Hint

```sql
-- 强制 Broadcast Join
SELECT /*+ SET_VAR(exec_mem_limit=8589934592) */
       ...
FROM big_table t1
JOIN [broadcast] small_table t2
    ON t1.key = t2.key;

-- 强制 Shuffle Join
SELECT ...
FROM big_table t1
JOIN [shuffle] big_table_2 t2
    ON t1.key = t2.key;
```

---

## 5. Profile 运行时分析

EXPLAIN 是静态分析，Profile 是运行时实际数据。ETL 上线后可通过 Profile 验证：

### 5.1 启用 Profile

```sql
SET is_report_success = true;
-- 然后执行 ETL SQL
{etl_sql};
```

### 5.2 查看 Profile

```sql
-- 查看最近的查询 Profile
SHOW QUERY PROFILE '/';

-- 查看指定 Query ID 的 Profile
SHOW QUERY PROFILE '/{query_id}';

-- 查看指定 Fragment 的 Profile
SHOW QUERY PROFILE '/{query_id}/{fragment_id}';
```

### 5.3 Profile 关注指标

| 指标 | 含义 | 关注 |
|------|------|------|
| `TotalTime` | 总执行时间 | 是否满足 SLA |
| `ScanRows` | 实际扫描行数 | 与 EXPLAIN 估算是否一致 |
| `ReturnRows` | 返回行数 | 聚合效果 |
| `PeakMemoryUsage` | 峰值内存 | 是否接近 `exec_mem_limit` |
| `BytesRead` | 读取数据量 | 分区裁剪效果 |
| `NetworkTime` | 网络传输时间 | Shuffle 开销 |
| `WaitTime` | 等待时间 | 资源竞争 |

---

## 6. 性能基线建议

对于接入调度的 ETL 任务，建议设定性能基线：

| 场景 | 期望延迟 | Profile 检查 |
|------|---------|-------------|
| 实时大屏查询 | < 3s | TotalTime < 3000ms |
| 交互式报表 | < 10s | TotalTime < 10000ms |
| 批量 ETL 写入 | < 5min | TotalTime < 300000ms |
| 宽表物化 | < 30min | PeakMemoryUsage < exec_mem_limit * 80% |
