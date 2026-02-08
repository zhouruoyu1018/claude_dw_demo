---
name: review-sql
description: SQL 审查。对任意已有 DDL/ETL SQL 脚本进行代码规范审查，输出分级审查报告（FATAL/ERROR/WARN/INFO）及修复建议。使用场景：(1) 接手别人的 ETL 脚本，检查是否符合项目规范 (2) 重构现有表，评估现有 SQL 的问题 (3) Code Review 场景，PR 中的 SQL 自动化审查 (4) dw-dev-workflow Phase 4.5 门禁检查
---

# SQL 审查 (Review SQL)

对任意已有 DDL 或 ETL SQL 脚本进行**代码规范审查**，输出结构化审查报告。

## 定位

**审核员（Reviewer）** — 不负责生成代码，只负责检查现有代码是否符合项目规范。

**与其他 Skill 的区别**：
- `generate-etl-sql` Step 2.5 自检 → 仅作用于工作流内新生成的 SQL
- `generate-qa-suite` → 运行时数据质量检查（DQC），不是代码审查
- **review-sql** → 对**任意已有 SQL 脚本**做代码规范审查

## 输入输出

### 输入

| 来源 | 内容 | 必需 |
|------|------|------|
| 用户 | SQL 脚本（文件路径或粘贴内容） | 是 |
| 用户 | 目标引擎（Hive/Impala/Doris） | 可选，自动识别 |

### 输出

结构化审查报告，按严重级降序排列，包含：
- 基本信息（脚本类型、引擎、时间）
- 审查结果汇总（FATAL/ERROR/WARN/INFO/PASS 计数）
- 详细发现（规则ID + 位置 + 问题描述 + 修复建议）
- 结论与行动建议

输出格式见 [assets/output-template.md](assets/output-template.md)。

---

## 核心工作流

```
输入 SQL 脚本
    ↓
┌──────────────────────────────┐
│ Step 1: 识别脚本类型          │
│ DDL / ETL / 混合             │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 2: 加载检查规则          │
│ ├─ DDL  → D-01 ~ D-08       │
│ ├─ ETL  → E-01 ~ E-10       │
│ └─ 通用 → S-01 ~ S-04       │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 3: 逐项检查              │
│ 标注 PASS / FATAL / ERROR /  │
│ WARN / INFO                  │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 生成审查报告          │
│ 按严重级降序排列              │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 5: 输出修复建议          │
│ 对每个非 PASS 项给出修改方案 │
└──────────────────────────────┘
    ↓
输出审查报告
```

---

## Step 1: 识别脚本类型

根据 SQL 关键字自动判断：

| 关键字 | 类型 |
|--------|------|
| `CREATE TABLE` / `ALTER TABLE` | DDL |
| `INSERT` / `SELECT ... FROM` | ETL |
| 同时包含 DDL + DML 语句 | 混合 |

混合类型时，DDL 和 ETL 规则**均适用**。

### 引擎识别

| 特征 | 引擎 |
|------|------|
| `${hivevar:...}` / `SET hive.` / `STORED AS ORC` | Hive |
| `${var:...}` / `SET MEM_LIMIT` / `COMPUTE STATS` | Impala |
| `DUPLICATE KEY` / `DISTRIBUTED BY` / `PROPERTIES("replication_num")` | Doris |
| 无法识别 | 标注"未识别"，跳过引擎相关检查 (E-10) |

---

## Step 2: 审查规则摘要

### A. DDL 审查（CREATE / ALTER TABLE 时触发）

| 规则ID | 检查项 | 严重级 |
|--------|--------|--------|
| D-01 | 表名是否符合 `{层}_{主题}_{粒度}` 命名规范 | ERROR |
| D-02 | 字段名是否基于词根表 | WARN |
| D-03 | 所有字段是否有 COMMENT | ERROR |
| D-04 | COMMENT 中是否包含粒度标注 `[粒度：...]` | WARN |
| D-05 | 是否有 TBLPROPERTIES（logical_primary_key 等） | WARN |
| D-06 | 分区字段是否为标准名称（日分区 `stat_date`，月分区 `stat_month`） | WARN |
| D-07 | 字段排序是否符合规范（维度 → 布尔 → 指标） | INFO |
| D-08 | 数据类型是否合理（金额用 DECIMAL、计数用 BIGINT 等） | WARN |

### B. ETL SQL 审查（INSERT / SELECT 时触发）

| 规则ID | 检查项 | 严重级 |
|--------|--------|--------|
| E-01 | 主表 WHERE 是否包含分区过滤（`stat_date` / `stat_month` 等分区字段） | FATAL |
| E-02 | JOIN 条件是否完整（维度表也需分区过滤） | ERROR |
| E-03 | 是否存在 N:N JOIN 风险 | ERROR |
| E-04 | 分母为 0 是否处理（NULLIF / CASE WHEN） | ERROR |
| E-05 | 聚合字段 NULL 是否处理（COALESCE） | WARN |
| E-06 | GROUP BY 与 SELECT 非聚合字段是否一致 | FATAL |
| E-07 | 窗口函数是否有 PARTITION BY 和 ORDER BY | ERROR |
| E-08 | 日期参数是否参数化（非硬编码） | ERROR |
| E-09 | CTE 命名是否有业务含义 | INFO |
| E-10 | 引擎语法是否与目标引擎匹配 | WARN |

### C. 规范审查（DDL + ETL 通用）

| 规则ID | 检查项 | 严重级 |
|--------|--------|--------|
| S-01 | 脚本头部是否有标准注释块 | WARN |
| S-02 | 层级是否在 dm/da 范围内 | ERROR |
| S-03 | SELECT * 使用 | ERROR |
| S-04 | GROUP BY 使用列序号（GROUP BY 1,2） | WARN |

每条规则的**详细判断逻辑和示例**见 [references/review-checklist.md](references/review-checklist.md)。

---

## Step 3: 逐项检查

对每条规则，逐一检查并标注结果：

| 结果 | 含义 |
|------|------|
| **PASS** | 检查通过 |
| **FATAL** | 阻断问题，必须修复后才能上线 |
| **ERROR** | 严重问题，需要修复 |
| **WARN** | 建议改善 |
| **INFO** | 信息提示，可选改进 |

检查时需关注：
- **定位行号**：标注问题所在的具体行号或 SQL 位置
- **上下文**：引用相关代码片段
- **判断依据**：说明为何判定为该级别

---

## Step 4: 生成审查报告

按 [assets/output-template.md](assets/output-template.md) 模板输出，核心结构：

1. **基本信息**：脚本路径、类型、引擎、时间
2. **汇总表**：各级别数量统计
3. **详细发现**：按 FATAL → ERROR → WARN → INFO 降序排列
4. **结论**：阻断项数量 + 行动建议

---

## Step 5: 输出修复建议

对每个非 PASS 项，给出**具体可操作的修复方案**：

| 问题类型 | 修复建议格式 |
|---------|-------------|
| 缺失项 | 给出需要**添加**的完整代码片段 |
| 错误项 | 给出**修改前 → 修改后**的对比 |
| 规范项 | 给出**符合规范的写法**示例 |

---

## 严重级定义

| 级别 | 含义 | 上线影响 |
|------|------|---------|
| **FATAL** | 逻辑错误，可能导致数据错误或全表扫描 | 阻断上线 |
| **ERROR** | 规范违反或潜在风险，需要修复 | 建议修复后上线 |
| **WARN** | 不影响正确性，但不符合最佳实践 | 可延后改善 |
| **INFO** | 信息提示，改善可读性和维护性 | 仅供参考 |

---

## 复用引用

本 Skill 的检查规则基于以下已有规范，不重复维护：

| 引用内容 | 来源 |
|---------|------|
| 表/字段命名规范 | `generate-standard-ddl/references/naming-convention.md` |
| 引擎语法差异 | `generate-etl-sql/references/engine-syntax.md` |
| SQL 模式与反模式 | `generate-etl-sql/references/sql-patterns.md` |
| 检查规则详细判断逻辑 | 本 Skill `references/review-checklist.md` |

---

## 使用方式

### 方式一：独立调用

```
用户: /review-sql
     [粘贴 SQL 脚本或提供文件路径]

助手: [执行审查流程，输出报告]
```

### 方式二：指定文件

```
用户: /review-sql sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql
```

### 方式三：dw-dev-workflow 集成

在 dw-dev-workflow 中使用 `--review` 参数，ETL 生成后自动触发审查：

```
/dw-dev-workflow --review
```

---

## 交互式确认

以下情况主动询问用户：

1. **引擎不明确**: "未检测到明确的引擎标识，请确认目标引擎（Hive/Impala/Doris）？"
2. **层级不明确**: "表名未包含明确的层级前缀（dm_/da_），是否需要检查 S-02 规则？"
3. **FATAL 修复确认**: "发现 {N} 个阻断问题，是否需要我直接生成修复后的完整脚本？"

---

## 与其他 Skill 的协作

```
[独立使用]
任意 SQL 脚本 → review-sql → 审查报告

[工作流集成]
generate-etl-sql → review-sql (Phase 4.5) → generate-qa-suite
```

### 协作场景

| 场景 | 流程 |
|------|------|
| 独立审查 | 用户提供 SQL → review-sql 输出报告 |
| 新开发审查 | generate-etl-sql → review-sql → generate-qa-suite |
| 修复后复查 | review-sql 发现问题 → 用户修复 → review-sql 复查 |

## References

- [references/review-checklist.md](references/review-checklist.md) - 完整检查规则清单（详细判断逻辑与示例）
- [assets/output-template.md](assets/output-template.md) - 审查报告输出模板
