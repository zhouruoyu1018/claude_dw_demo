---
name: generate-qa-suite
description: 测试与 DQC 生成。根据 ETL SQL 和业务需求，自动生成冒烟测试 SQL、数据质量校验（DQC）规则和 Doris 性能分析请求。使用场景：(1) ETL 脚本开发完成后需要验证正确性 (2) 上线前数据质量卡点检查 (3) 日常调度后自动化质量巡检 (4) Doris 查询性能分析与优化
---

# 测试与 DQC 生成 (Generate QA Suite)

根据 ETL SQL 代码和业务需求，自动生成完整的质量验证套件。

## 定位

**质检员** — 上游 Skill 完成"施工"（ETL SQL），本 Skill 负责"验收"（测试 + 质量检查 + 性能分析）。

## 输入输出

### 输入

| 来源 | 内容 | 必需 |
|------|------|------|
| `generate-etl-sql` | ETL SQL 脚本（增量 `_etl.sql` 或初始化 `_init.sql`） | 是 |
| `generate-standard-ddl` | 目标表 DDL（含逻辑主键、字段类型、COMMENT） | 是 |
| 业务需求 / `dw-requirement-triage` | 业务规则、指标口径、预期范围 | 是 |
| 引擎类型 | Hive / Impala / Doris | 是 |

### 输出

完整的 QA 套件，包含三部分：

```
[Part 1] 冒烟测试 SQL     — ETL 执行后立即跑，快速判断是否正常
[Part 2] DQC 规则 SQL      — 系统化质量检查，可接入调度平台
[Part 3] 性能分析（Doris）  — EXPLAIN 分析 + 优化建议
```

---

## 核心工作流

```
ETL SQL + DDL + 业务需求
    ↓
┌──────────────────────────────┐
│ Step 1: 解析 ETL 上下文      │
│ 提取源表/目标表/主键/字段   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 2: 生成冒烟测试         │
│ 行数 / 主键 / 空值 / 样本   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 3: 生成 DQC 规则        │
│ 按字段类型 + 业务语义匹配   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 性能分析（Doris）    │
│ EXPLAIN + 瓶颈识别          │
└──────────────────────────────┘
    ↓
输出完整 QA 套件
```

---

## Step 1: 解析 ETL 上下文

从 ETL SQL 脚本和目标表 DDL 中自动提取：

| 提取项 | 来源 | 示例 |
|--------|------|------|
| 目标表 | `INSERT OVERWRITE TABLE ...` | `dm.dmm_sac_loan_prod_daily` |
| 源表列表 | `FROM` / `JOIN` 子句 | `dwd.dwd_loan_detail`, `dim.dim_product` |
| 逻辑主键 | DDL TBLPROPERTIES `logical_primary_key` | `product_code, stat_date` |
| 分区字段 | DDL `PARTITIONED BY` | `stat_date` |
| 维度字段 | DDL 注释分组"维度字段" | `product_code`, `product_name` |
| 指标字段 | DDL 注释分组"指标字段" | `td_sum_loan_amt`, `td_cnt_loan` |
| 布尔字段 | `is_` / `has_` 前缀 | `is_first_overdue` |
| 金额字段 | 类型 `DECIMAL(18,2)` + COMMENT 含"金额" | `td_sum_loan_amt` |
| 比率字段 | 类型 `DECIMAL(10,4)` + COMMENT 含"率" | `rat_overdue_m1` |
| 计数字段 | 类型 `BIGINT` + COMMENT 含"笔数/件数" | `td_cnt_loan` |
| 引擎类型 | 脚本 SET 参数或用户指定 | Hive / Impala / Doris |
| 脚本类型 | 文件名 `_init.sql` 或动态分区特征 | 增量 / 初始化 |

### 初始化脚本 (init) 的测试差异

当输入为 `_init.sql` 时，测试生成须适配以下差异：

| 测试项 | 增量脚本 | 初始化脚本 |
|--------|---------|-----------|
| **冒烟测试分区** | 验证单个 `stat_date = '${stat_date}'` | 验证日期范围 `stat_date BETWEEN '${start_date}' AND '${end_date}'` 内多个分区均有数据 |
| **分区完整性** | 不需要 | 追加检查：回刷范围内每个日期分区均存在且行数 > 0 |
| **跨分区一致性** | 不需要 | 追加检查：相邻日期分区的行数波动率不超过阈值（默认 50%） |
| **动态分区配置** | 不需要 | 追加检查：脚本包含 `SET hive.exec.dynamic.partition=true` |

---

## Step 2: 生成冒烟测试

冒烟测试用于 ETL 执行后**快速验证**，判断本次运行是否正常。应在 1 分钟内完成。

### 2.1 测试项清单

| 编号 | 测试项 | 目的 | 严重级别 |
|------|--------|------|---------|
| S-01 | 目标表行数 > 0 | 确认数据已写入 | **FATAL** |
| S-02 | 源表行数 vs 目标表行数 | 聚合比例是否合理 | WARN |
| S-03 | 主键唯一性 | 无重复数据 | **FATAL** |
| S-04 | 关键字段非 NULL | 维度和核心指标不为空 | ERROR |
| S-05 | 数据样本抽查 | 人工可视化检查 | INFO |
| S-06 | 分区写入确认 | 目标分区存在 | **FATAL** |

### 2.2 SQL 生成模板

```sql
-- ============================================================
-- 冒烟测试: {target_table}
-- ETL 日期: ${stat_date}
-- 生成时间: {YYYY-MM-DD HH:MM:SS}
-- ============================================================

-- [S-01] 目标表行数 (FATAL: 必须 > 0)
SELECT 'S-01: 目标表行数' AS test_id,
       COUNT(*)           AS result,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM {target_table}
WHERE stat_date = '${stat_date}';

-- [S-02] 源表行数对比 (WARN: 比例异常时预警)
SELECT 'S-02: 行数对比' AS test_id,
       src_cnt, tgt_cnt,
       ROUND(tgt_cnt / NULLIF(src_cnt, 0), 4) AS ratio,
       CASE
           WHEN tgt_cnt = 0 THEN 'FATAL'
           WHEN tgt_cnt > src_cnt THEN 'WARN: 目标 > 源表，检查是否 JOIN 膨胀'
           ELSE 'PASS'
       END AS status
FROM (
    SELECT
        (SELECT COUNT(*) FROM {source_table} WHERE stat_date = '${stat_date}') AS src_cnt,
        (SELECT COUNT(*) FROM {target_table} WHERE stat_date = '${stat_date}') AS tgt_cnt
) t;

-- [S-03] 主键唯一性 (FATAL: 必须为 0)
SELECT 'S-03: 主键重复' AS test_id,
       COUNT(*)          AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FATAL' END AS status
FROM (
    SELECT {pk_cols}
    FROM {target_table}
    WHERE stat_date = '${stat_date}'
    GROUP BY {pk_cols}
    HAVING COUNT(*) > 1
) dup;

-- [S-04] 关键字段非 NULL (ERROR: 应为 0)
SELECT 'S-04: NULL值检查' AS test_id,
       {null_check_expression}   AS result,
       CASE WHEN {null_check_expression} = 0 THEN 'PASS' ELSE 'ERROR' END AS status
FROM {target_table}
WHERE stat_date = '${stat_date}';

-- [S-05] 数据样本 (INFO: 人工检查)
SELECT *
FROM {target_table}
WHERE stat_date = '${stat_date}'
LIMIT 20;

-- [S-06] 分区确认 (FATAL)
SHOW PARTITIONS {target_table};
-- 或 Hive:
-- SELECT DISTINCT stat_date FROM {target_table} WHERE stat_date = '${stat_date}';
```

### 2.3 NULL 检查表达式生成

根据字段重要性自动选择需要检查的字段：

```sql
-- 维度字段 + 核心指标全部检查
SUM(
    CASE WHEN {dim_col_1} IS NULL THEN 1 ELSE 0 END
  + CASE WHEN {dim_col_2} IS NULL THEN 1 ELSE 0 END
  + CASE WHEN {metric_col_1} IS NULL THEN 1 ELSE 0 END
)
```

优先级：
- **必检**: 逻辑主键字段、分区字段
- **应检**: 维度字段（_code, _id）、核心指标字段
- **可选**: 名称字段（_name）、辅助指标

---

## Step 3: 生成 DQC 规则

DQC 规则是系统化的质量检查，可接入调度平台（如 Airflow、DolphinScheduler）作为下游卡点。

### 3.1 规则自动匹配

根据字段类型和命名模式，自动匹配适用的 DQC 规则：

| 字段特征 | 自动匹配的规则 |
|---------|---------------|
| 逻辑主键 (`logical_primary_key`) | 唯一性检查 |
| 金额字段 (`DECIMAL` + `_amt`) | 非负检查、合理范围检查 |
| 比率字段 (`DECIMAL` + `_rat`/`_rate`) | [0, 1] 范围检查 |
| 计数字段 (`BIGINT` + `_cnt`) | 非负检查 |
| 布尔字段 (`TINYINT` + `is_`/`has_`) | {0, 1} 枚举检查 |
| 日期字段 (`STRING` + `_date`) | 格式检查（YYYY-MM-DD） |
| 维度编码 (`STRING` + `_code`) | 非空检查、引用完整性 |
| 所有指标 | 波动率检查（T vs T-1） |
| 全表 | 行数非零、行数波动 |

### 3.2 规则分类与模板

根据字段特征自动匹配规则，每类规则的完整 SQL 模板见 [references/dqc-rules-catalog.md](references/dqc-rules-catalog.md)。

| 规则类别 | 规则编号 | 检查内容 | 适用字段特征 |
|---------|---------|---------|------------|
| 完整性 | DQC-C01 | 表非空 | 全表 |
| 完整性 | DQC-C02 | 非 NULL | 逻辑主键、维度编码 |
| 完整性 | DQC-C03 | 非空串 | STRING 类型维度字段 |
| 唯一性 | DQC-U01 | 主键唯一 | `logical_primary_key` |
| 有效性 | DQC-V01 | 非负 | 金额 `DECIMAL` + `_amt` |
| 有效性 | DQC-V02 | 范围 [0,1] | 比率 `DECIMAL` + `_rat` |
| 有效性 | DQC-V03 | 枚举 {0,1} | 布尔 `TINYINT` + `is_`/`has_` |
| 有效性 | DQC-V04 | 非负 | 计数 `BIGINT` + `_cnt` |
| 有效性 | DQC-V05 | 日期格式 | `STRING` + `_date` |
| 一致性 | DQC-CS01 | 引用完整 | 维度编码 → 维度表 |
| 一致性 | DQC-CS02 | 跨层金额一致 | 汇总金额 vs 明细金额 |
| 波动率 | DQC-VOL01 | 行数波动 | 全表 T vs T-1 (阈值 50%) |
| 波动率 | DQC-VOL02 | 指标波动 | 核心指标 T vs T-1 (阈值 100%) |

### 3.3 规则严重级别

| 级别 | 含义 | 调度动作 |
|------|------|---------|
| **FATAL** | 数据不可用 | 阻断下游任务，立即告警 |
| **ERROR** | 数据质量严重问题 | 告警，人工确认后放行 |
| **WARN** | 数据波动异常 | 告警，不阻断 |
| **INFO** | 信息性检查 | 仅记录 |

### 3.4 规则输出汇总表

所有规则结果写入统一汇总：

```sql
-- DQC 结果汇总
SELECT
    '${stat_date}'   AS check_date,
    rule_id,
    target,
    expected,
    actual,
    result,
    CURRENT_TIMESTAMP AS check_time
FROM (
    -- 各规则 UNION ALL
    {rule_1}
    UNION ALL
    {rule_2}
    UNION ALL
    ...
) all_rules
ORDER BY
    CASE result
        WHEN 'FATAL' THEN 1
        WHEN 'FAIL'  THEN 2
        WHEN 'ERROR' THEN 3
        WHEN 'WARN'  THEN 4
        ELSE 5
    END;
```

---

## Step 4: 性能分析（Doris 专项）

当目标引擎为 Doris 时，额外生成性能分析请求。

### 4.1 EXPLAIN 分析

对 ETL 查询生成三级 EXPLAIN：`EXPLAIN` / `EXPLAIN VERBOSE` / `EXPLAIN GRAPH`

### 4.2 关注点速查

| 算子 | 关注点 | 优化方向 |
|------|--------|---------|
| `OlapScanNode` | 扫描行数 | 分区裁剪、物化视图 |
| `HASH JOIN` | 右表大小 | Broadcast vs Shuffle |
| `EXCHANGE` | Shuffle 量 | Colocate Join |
| `Predicates` | 谓词下推 | 确认分区/桶裁剪生效 |

详见 [references/doris-explain-guide.md](references/doris-explain-guide.md) 获取 Profile 分析、Tablet/Compaction 检查等完整方法。

---

## 引擎适配

### 正则函数差异

| 功能 | Hive | Impala | Doris |
|------|------|--------|-------|
| 正则匹配 | `col RLIKE 'pattern'` | `col REGEXP 'pattern'` | `col REGEXP 'pattern'` |
| 日期函数 | `DATE_ADD(stat_date, -1)` | `DAYS_SUB(stat_date, 1)` | `DATE_SUB(partition_key, INTERVAL 1 DAY)` |
| 分区查询 | `SHOW PARTITIONS t` | `SHOW PARTITIONS t` | `SHOW PARTITIONS FROM t` |
| EXPLAIN | `EXPLAIN` | `EXPLAIN` | `EXPLAIN` / `EXPLAIN VERBOSE` / `EXPLAIN GRAPH` |

### 冒烟测试引擎适配

- **Hive**: 使用 `${hivevar:stat_date}`，`RLIKE` 正则
- **Impala**: 使用 `${var:stat_date}`，`REGEXP` 正则
- **Doris**: 硬编码日期或应用层替换，`REGEXP` 正则，额外输出 EXPLAIN

---

## 完整示例

**目标表**: `dm.dmm_sac_loan_prod_daily`，**逻辑主键**: `product_code, stat_date`，**引擎**: Hive

**自动规则匹配结果：**

| 字段 | 特征 | 命中规则 |
|------|------|---------|
| `product_code, stat_date` | 逻辑主键 | DQC-U01 |
| `product_code` | 维度编码 `_code` | DQC-C02, DQC-CS01 |
| `td_sum_loan_amt` | 金额 `DECIMAL` + `_amt` | DQC-V01, DQC-VOL02 |
| `td_cnt_loan` | 计数 `BIGINT` + `_cnt` | DQC-V04 |
| `is_first_loan` | 布尔 `TINYINT` + `is_` | DQC-V03 |
| `rat_overdue_m1` | 比率 `DECIMAL(10,4)` + `rat_` | DQC-V02 |
| 全表 | — | S-01, S-02, DQC-VOL01 |

根据匹配结果，自动生成包含 Part 1 冒烟测试 (S-01~S-06) + Part 2 DQC 规则的完整 SQL 脚本。

---

## 踩坑记录联动 (Pitfalls Integration)

当用户报告 DQC 规则捕获了**真实数据缺陷**（而非规则本身的误报），自动将该缺陷记录到 auto memory 目录下的 `pitfalls.md` 的"DQC 捕获的真实缺陷"区。

**记录格式**：
```markdown
### D-{序号}: {缺陷简述}
- 日期: {当天日期}
- 表: {目标表}
- 规则: {DQC 规则编号}
- 原因: {根因分析}
- 修正: {ETL 侧的修正方式}
```

**触发条件**：用户在 review QA 结果时确认某条 DQC FAIL/WARN 是真实问题（非预期行为）。

---

## 交互式确认

遇到以下情况时，主动询问用户：

1. **波动阈值**: "行数/指标波动超过多少算异常？默认行数 50%、指标 100%，需要调整吗？"

2. **比率范围**: "字段 `rat_overdue_m1` 默认检查 [0, 1] 范围。该比率是否可能超过 1（如以百分比存储）？"

3. **引用完整性**: "`product_code` 应关联哪张维度表做引用完整性检查？"

4. **跨层一致性**: "是否需要校验目标表指标汇总值与源表明细汇总值一致？"

5. **性能要求**: "Doris 查询是否有延迟要求？（如 < 3 秒），以便设定 EXPLAIN 性能基线。"

---

## 与其他 Skill 的协作

```
需求文档
    ↓
dw-requirement-triage            ← 需求拆解
    ↓
search-hive-metadata             ← 元数据搜索 + 指标复用
    ↓
generate-standard-ddl            ← 目标表 DDL
    ↓
generate-etl-sql                 ← ETL SQL
    ↓
generate-qa-suite                ← 本 Skill: 测试 + DQC + 性能分析
    ↓
调度上线（QA Suite 嵌入调度 DAG）
```

## References

- [references/dqc-rules-catalog.md](references/dqc-rules-catalog.md) - DQC 规则完整目录与阈值配置
- [references/doris-explain-guide.md](references/doris-explain-guide.md) - Doris EXPLAIN 执行计划解读指南
