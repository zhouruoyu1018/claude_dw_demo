# Hive/Impala/Doris 引擎选择指南

## 引擎特性对比

| 特性 | Hive (Tez) | Impala | Doris |
|-----|-----------|--------|-------|
| **定位** | 批处理ETL | 交互式查询 | 实时OLAP |
| **延迟** | 分钟~小时级 | 秒~分钟级 | 毫秒~秒级 |
| **并发** | 低 (10-50) | 中 (50-200) | 高 (1000+) |
| **数据量** | PB级 | TB级 | TB级 |
| **实时性** | T+1 | 准实时 | 实时 |
| **SQL兼容** | HiveQL | Impala SQL | MySQL协议 |
| **适用场景** | 复杂ETL、大批量 | 即席查询、BI | 实时报表、高并发 |

## 场景选择决策表

### 按需求类型选择

| 需求类型 | 首选引擎 | 备选引擎 | 说明 |
|---------|---------|---------|------|
| 复杂ETL转换 | Hive | - | 多表JOIN、复杂UDF |
| T+1报表 | Hive → Impala | Doris | Hive计算，Impala查询 |
| 即席分析 | Impala | Doris | 分析师自助查询 |
| 实时大屏 | Doris | - | 秒级刷新 |
| 高并发API | Doris | - | 1000+ QPS |
| 明细数据导出 | Hive | Impala | 大数据量导出 |
| 多维分析(OLAP) | Doris | Impala | 下钻、切片 |

### 按数据特征选择

| 数据特征 | 推荐引擎 | 原因 |
|---------|---------|------|
| 数据量 > 10TB | Hive | 批处理能力强 |
| 数据量 < 1TB | Impala/Doris | 查询更快 |
| 需要实时写入 | Doris | 支持实时导入 |
| 历史全量分析 | Hive | 成本低、稳定 |
| 需要秒级响应 | Doris | MPP架构 |

### 按用户角色选择

| 用户角色 | 推荐引擎 | 场景 |
|---------|---------|------|
| 数据工程师 | Hive | ETL开发 |
| 数据分析师 | Impala | 即席查询 |
| 业务运营 | Doris | 报表查看 |
| 产品经理 | Impala/Doris | 数据验证 |

## Doris 表模型选择

### Aggregate Model (聚合模型)

**适用场景**：
- 预聚合指标查询
- 只关心汇总结果，不需要明细
- 典型的 OLAP 场景

**支持的聚合函数**：
- SUM, MAX, MIN, COUNT
- REPLACE (保留最新值)
- REPLACE_IF_NOT_NULL
- HLL_UNION (近似去重)
- BITMAP_UNION (精确去重)

**示例**：
```sql
CREATE TABLE da_sales_daily (
    dt DATE,
    product_id BIGINT,
    sales_amount DECIMAL(18,2) SUM,
    order_count BIGINT SUM,
    max_price DECIMAL(18,2) MAX
)
AGGREGATE KEY(dt, product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 10;
```

### Unique Model (唯一模型)

**适用场景**：
- 需要 Upsert 语义
- 维度表（用户、商品、门店）
- 需要更新历史数据

**特点**：
- 相同 Key 只保留最新一条
- 支持部分列更新

**示例**：
```sql
CREATE TABLE dim_user (
    user_id BIGINT,
    user_name VARCHAR(100),
    phone VARCHAR(20),
    update_time DATETIME
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
```

### Duplicate Model (明细模型)

**适用场景**：
- 日志数据
- 需要保留完整明细
- 不需要去重或聚合

**特点**：
- 数据只追加不更新
- 查询时可灵活聚合

**示例**：
```sql
CREATE TABLE ods_click_log (
    event_time DATETIME,
    user_id BIGINT,
    page_id VARCHAR(50),
    click_position VARCHAR(20)
)
DUPLICATE KEY(event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
```

## 数仓分层与引擎对应

```
┌─────────────────────────────────────────────────────────┐
│  DA (应用层)                                             │
│  ├─ Doris: 实时报表、高并发接口                           │
│  └─ Impala: 交互式分析、BI工具对接                        │
├─────────────────────────────────────────────────────────┤
│  DWS (汇总层)                                            │
│  ├─ Hive: T+1汇总计算                                    │
│  └─ Doris Aggregate: 实时预聚合                          │
├─────────────────────────────────────────────────────────┤
│  DWD (明细层)                                            │
│  ├─ Hive: 主存储、历史数据                                │
│  └─ Doris Duplicate: 近期热数据                          │
├─────────────────────────────────────────────────────────┤
│  ODS (原始层)                                            │
│  ├─ Hive: 批量同步、历史归档                              │
│  └─ Doris: 实时接入 (Kafka/Stream Load)                  │
└─────────────────────────────────────────────────────────┘
```

## 混合架构最佳实践

### 模式1: Hive + Impala (共享存储)

```
数据源 → Hive (ETL) → HDFS/Hive表 → Impala (查询)
                                   ↓
                            INVALIDATE METADATA
```

**注意事项**：
- Hive DDL 后需在 Impala 执行 `INVALIDATE METADATA`
- 避免使用 Impala 不支持的 Hive UDF
- 分区表建议使用 `REFRESH table PARTITION`

### 模式2: Hive + Doris (数据同步)

```
数据源 → Hive (ETL) → DWD/DWS → Doris (Broker Load)
                                     ↓
                               实时查询服务
```

**同步方式**：
- Broker Load: 批量导入 HDFS 数据
- Routine Load: Kafka 实时消费
- Stream Load: HTTP 接口导入

### 模式3: 全链路 Doris

```
数据源 → Doris ODS (Stream Load) → Doris DWD → Doris DA
              ↓                        ↓            ↓
         Kafka实时              SQL转换      实时报表
```

**适用场景**：
- 实时性要求高
- 数据量 < 10TB
- 需要简化架构

## 性能优化建议

### Hive 优化
- 使用 ORC/Parquet 格式
- 合理设置分区 (dt)
- 开启向量化执行
- 小文件合并

### Impala 优化
- 收集统计信息 `COMPUTE STATS`
- 避免 `SELECT *`
- 使用分区裁剪
- 控制并发数

### Doris 优化
- 选择合适的表模型
- 合理设置分桶数
- 使用 Colocate Join
- 物化视图预聚合
