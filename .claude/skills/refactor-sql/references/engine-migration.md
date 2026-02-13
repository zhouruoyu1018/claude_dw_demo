# Hive -> Impala Migration Rules

## 目录

1. 迁移前提
2. 规则 M01-M06
3. 常用函数映射
4. 不兼容语法替代
5. 迁移校验清单

## 1. 迁移前提

1. 仅处理 Hive 到 Impala 的 SQL 改写
2. 目标引擎必须明确为 `impala`
3. 迁移过程中优先保证业务语义一致，其次再做性能优化
4. 更完整的差异矩阵参考 `generate-etl-sql/references/engine-syntax.md`

---

## 2. 规则 M01-M06

### M01: SET 参数清理/替换

- 识别对象: `SET hive.*`, `SET tez.*`, `SET mapreduce.*`
- 改写动作:
  - 删除 Impala 不支持的 Hive/Tez 参数
  - 保留或替换为 Impala 参数（如 `MEM_LIMIT`）

常见 Hive SET 参数处置映射：

| Hive SET 参数 | 处置 | Impala 替代 | 说明 |
|---------------|------|-------------|------|
| `hive.exec.dynamic.partition=true` | 删除 | — | Impala 默认支持动态分区 |
| `hive.exec.dynamic.partition.mode=nonstrict` | 删除 | — | Impala 无此限制 |
| `hive.exec.parallel=true` | 删除 | — | Impala 自动并行 |
| `hive.exec.compress.output=true` | 删除 | `COMPRESSION_CODEC=snappy` | 按需替换（通常表级设置） |
| `hive.map.aggr=true` | 删除 | — | Impala 自动 map-side 聚合 |
| `hive.auto.convert.join=true` | 删除 | — | Impala 自动选择 broadcast |
| `hive.auto.convert.join.noconditionaltask.size` | 删除 | — | 对应 Impala `BROADCAST_BYTES_LIMIT`（通常不需设） |
| `mapreduce.job.reduces=N` | 删除 | — | Impala 不使用 MapReduce |
| `mapreduce.map.memory.mb=N` | 替换 | `SET MEM_LIMIT=Ng` | 换算为 Impala 内存限制 |
| `tez.am.resource.memory.mb` | 替换 | `SET MEM_LIMIT=Ng` | 换算为 Impala 内存限制 |
| `tez.grouping.min-size` / `max-size` | 删除 | — | Impala 无对应参数 |
| `hive.vectorized.execution.enabled` | 删除 | — | Impala 默认向量化执行 |

```sql
-- before (Hive)
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.map.memory.mb=4096;

-- after (Impala)
SET MEM_LIMIT=4g;
```

### M02: 函数改写

- 识别对象: Hive 特有函数/参数形式
- 改写动作: 用 Impala 等价函数替换

```sql
-- before
SELECT DATE_ADD(stat_date, 1), NVL(amt, 0) FROM t;

-- after
SELECT DAYS_ADD(stat_date, 1), IFNULL(amt, 0) FROM t;
```

### M03: Hint 改写

- 识别对象: `/*+ MAPJOIN(t) */`
- 改写动作: Impala 等价 hint（`/*+ BROADCAST(t) */` 或 `[BROADCAST]`）
- 注意: 若集群策略禁止 hint，保留注释并在报告中标风险

### M04: 数据类型适配

- 识别对象: Hive 常见宽松类型定义
- 改写动作:
  - STRING 与 VARCHAR 场景化替换
  - TINYINT 溢出风险校验
  - DECIMAL 精度显式化

### M05: 语法差异改写

- 识别对象: `SORT BY` / `CLUSTER BY` / `DISTRIBUTE BY`
- 改写动作:
  - `SORT BY`、`CLUSTER BY` 迁移为 `ORDER BY`（结合语义）
  - `DISTRIBUTE BY` 改写为可在 Impala 执行的分发语义（必要时拆分步骤）

### M06: 分区语法改写

- 识别对象: Hive 动态分区依赖 SET 参数与写法
- 改写动作:
  - 删除 Hive 动态分区 SET
  - 改为 Impala 可执行的分区 INSERT 语法
- 注意: Impala 的分区写入和变量写法需同步调整

---

## 3. 常用函数映射

| Hive | Impala | 说明 |
|------|--------|------|
| `DATE_ADD(d, n)` | `DAYS_ADD(d, n)` | 日期加天 |
| `DATE_SUB(d, n)` | `DAYS_SUB(d, n)` | 日期减天 |
| `NVL(x, y)` | `IFNULL(x, y)` | NULL 处理 |
| `${hivevar:stat_date}` | `${var:stat_date}` | 参数变量 |
| `FROM_UNIXTIME(ts)` | `FROM_UNIXTIME(ts)` | 保持一致（注意格式串差异） |

优先使用三引擎通用写法（如 `COALESCE`）以降低后续迁移成本。

---

## 4. 不兼容语法替代

### 4.1 LATERAL VIEW

- Hive `LATERAL VIEW explode()` 在 Impala 可能需改为 `UNNEST` 或改写为中间表
- 若无法一对一改写，输出“需人工确认”并保留原逻辑注释

### 4.2 Hive 专属 UDF

- 自定义 UDF 无 Impala 对应实现时：
  1. 优先替换为内置函数组合
  2. 无替代时在报告中标记 `HIGH` 风险

### 4.3 多 INSERT 语句块

- Hive 单脚本多 INSERT 在 Impala 需按执行器能力拆分
- 拆分后保持同样过滤条件和字段语义

---

## 5. 迁移校验清单

迁移完成后必须逐项核验：

1. 结果集粒度与主键是否一致
2. 日期/时间函数返回类型是否一致
3. JOIN 逻辑是否保持等价
4. 分区过滤是否完整保留
5. 变量注入方式是否改为 Impala 风格
6. 所有 Hive 专属 SET/语法是否已清理

若任一项无法确认，报告中标记“需人工复核”，不要输出“可直接上线”结论。
