---
name: refactor-sql
description: SQL 重构与自动改写。针对已有 DDL/ETL SQL 执行结构优化、性能优化、合规修复与 Hive→Impala 迁移，输出重构后 SQL、重构报告和 A/B 验证 SQL。使用场景：(1) 接手存量 SQL 需要提质重构 (2) review-sql 已发现 FATAL/ERROR 需要批量修复 (3) 需要降低全表扫描、JOIN 倾斜、重复计算等性能风险 (4) Hive 脚本迁移到 Impala
---

# SQL 重构 (Refactor SQL)

读取已有 SQL，完成诊断、自动重写、等价性校验与影响分析。

## 定位

**重构师（Refactorer）**:
- 关注已有脚本的可维护性、性能与规范合规
- 在可控风险下自动改写代码并给出验证方案

**与其他 Skill 分工**:
- `review-sql`: 只审查，不改写
- `generate-etl-sql`: 从零生成，不面向存量改造
- **refactor-sql**: 面向存量 SQL 的重构与迁移

## 输入输出

### 输入

| 来源 | 内容 | 必需 |
|------|------|------|
| 用户 | SQL 文件路径或脚本内容 | 是 |
| 用户 | `--scope` (`all`/`code`/`perf`/`compliance`/`migrate`) | 否 |
| 用户 | `--target=impala`（仅 migrate） | migrate 时是 |
| 用户 | `--dry-run` / `--no-impact` | 否 |

### 输出

默认输出三类交付物：
1. 重构后 SQL（参考 [assets/output-template.sql](assets/output-template.sql)）
2. 重构报告（参考 [assets/output-report.md](assets/output-report.md)）
3. A/B 验证 SQL（写入报告）

若输入是文件路径，优先写入同目录新文件：
- `{original_name}_refactored.sql`
- `{original_name}_refactor_report.md`

## 重构范围（scope）

| scope | 说明 |
|------|------|
| `code` | 代码结构优化（可读性、模块化、重复逻辑消除） |
| `perf` | 性能优化（分区裁剪、JOIN 优化、聚合下推） |
| `compliance` | 自动修复 review-sql 的 FATAL/ERROR/WARN 项 |
| `migrate` | Hive → Impala 迁移改写 |
| `all`（默认） | `code + perf + compliance`（不含 migrate） |

## 核心工作流（7 步）

```
输入 SQL ──→ Step 0 ──→ Step 1 ──→ Step 2 ──→ Step 3 ──→ Step 4 ──→ Step 5 ──→ Step 6
            前置审查    范围确认    策略选择    自动重写    等价校验    影响分析    输出交付
            (review)   (交互)     (加载规则)  (改写)     (验证)     (MCP)      (落盘)
               │                                │          │          │
               │ scope=migrate                  │ dry-run   │ 失败     │ MCP不可用
               └──→ 跳过 review                 └──→ 仅报告 └──→ 人工  └──→ 跳过
```

### Step 0: 前置审查

1. 当 `scope != migrate` 时，先执行 `review-sql`：
   - 提取 FATAL/ERROR/WARN 作为优先改写候选
2. 补充静态诊断：
   - 子查询嵌套深度 > 2
   - 重复表达式重复出现 >= 2
   - `SELECT *`
   - 缺失分区过滤
   - JOIN 条件不完整或潜在 N:N
3. 当 `scope = migrate` 时，跳过 `review-sql`，直接进入迁移模式。

### Step 1: 确定重构范围并交互确认

基于 `--scope` 与审查结果生成改写清单，逐项标注风险等级：
- `安全`: 注释补全、字段显式化、CTE 命名
- `低`: NULL 安全函数替换
- `中`: JOIN 顺序调整、CTE 拆解
- `高`: 语义相关迁移改写、聚合路径调整

在实际改写前先给用户确认：
- `scope`
- 预计应用规则 ID
- 高风险项列表

#### 1.1 高风险改写预览

对风险等级为 `中` 或 `高` 的规则，必须在确认阶段输出 **before/after diff 预览**，让用户直观判断是否接受。格式：

```
┌─ [规则ID] 规则名称 ── 风险: 中/高 ──────────────────────
│
│  -- BEFORE (原始)
│  SELECT a.amt / b.cnt AS avg_amt
│  FROM fact_loan a
│  JOIN dim_product b ON a.prod_id = b.prod_id
│
│  -- AFTER (改写后)
│  WITH loan_agg AS (
│      SELECT prod_id, SUM(amt) AS total_amt
│      FROM fact_loan
│      WHERE stat_date = '${stat_date}'
│      GROUP BY prod_id
│  )
│  SELECT la.total_amt / NULLIF(b.cnt, 0) AS avg_amt
│  FROM loan_agg la
│  JOIN dim_product b ON la.prod_id = b.prod_id
│
│  变更说明: 先聚合后关联，减少 JOIN 数据量；补充安全除法
│  语义影响: 粒度从明细行变为产品维度聚合，需确认是否符合预期
└──────────────────────────────────────────────────────────
```

需要预览的规则（非全量，仅中/高风险）：

| 风险 | 规则 | 预览原因 |
|------|------|---------|
| 中 | C01 嵌套→CTE | 子查询拆解后执行顺序可能变化 |
| 中 | P02 JOIN 顺序 | 可能影响执行计划和结果顺序 |
| 中 | P03 广播 Hint | 小表判断错误会导致 OOM |
| 高 | P04 倾斜处理 | 两阶段聚合/盐值打散改变 SQL 结构 |
| 高 | P05 先聚合后关联 | 改变计算路径，粒度可能变化 |
| 高 | M02 函数改写 | 函数返回类型/精度可能不一致 |
| 高 | M05 语法差异 | SORT BY→ORDER BY 语义不完全等价 |

用户确认选项：
- **(A) 全部接受** — 按清单执行所有改写
- **(B) 逐项选择** — 对每个中/高风险项单独确认（接受/跳过/修改）
- **(C) 仅安全+低风险** — 跳过所有中/高风险项

### Step 2: 选择重构策略

1. 按 scope 读取 [references/refactoring-patterns.md](references/refactoring-patterns.md)
2. `migrate` 模式同时读取 [references/engine-migration.md](references/engine-migration.md)
3. 如需更完整引擎差异，补充引用 `generate-etl-sql/references/engine-syntax.md`

### Step 3: 自动重写

按固定顺序执行，避免模式冲突：

`compliance -> code -> perf`

#### 3.1 compliance 执行序列

按规则编号顺序逐项应用：

1. `F-Sxx`（通用规范）: F-S01 头部注释 → F-S02 分层范围 → F-S03 SELECT * → F-S04 GROUP BY 序号
2. `F-Dxx`（DDL 规范）: F-D01 表命名 → F-D02 字段词根 → … → F-D08 类型合理性
3. `F-Exx`（ETL 规范）: F-E01 主表分区 → F-E02 JOIN 分区 → … → F-E10 引擎语法

#### 3.2 code 执行序列

遵循"先结构后细节"原则：

1. `C01` 深层嵌套拆解为 CTE（先理清结构）
2. `C02` 重复逻辑抽取（在 CTE 基础上消除重复）
3. `C05` 语义化命名（对 CTE/别名统一命名）
4. `C03` 安全除法 + `C04` NULL 安全聚合（细节修补，可并行）
5. `C06` SELECT * 显式化（最后展开，避免中途干扰其他规则匹配）

#### 3.3 perf 执行序列

遵循"先裁剪后优化"原则：

1. `P01` 分区过滤补全 + `P06` 分区裁剪强化（先减少数据量）
2. `P03` 广播/MapJoin Hint（小表标记）
3. `P02` JOIN 顺序优化（调整关联顺序）
4. `P05` 先聚合后关联（改变计算路径）
5. `P04` 倾斜处理（最后处理，因为前面步骤可能已缓解倾斜）

#### 3.4 migrate 执行序列

使用 `M01~M06` 顺序：

1. `M01` SET 参数清理（先清理环境配置）
2. `M02` 函数改写（核心语义改写）
3. `M03` Hint 改写（执行计划 Hint）
4. `M04` 数据类型适配（类型兼容）
5. `M05` 语法差异改写（SORT BY 等）
6. `M06` 分区语法改写（分区写入语法）

#### 3.5 dry-run 模式

若指定 `--dry-run`：
- 不落盘改写结果
- 仅输出待改写清单、风险、示例 diff

### Step 4: 等价性校验

执行以下校验并在报告中记录：
1. 粒度与主键是否保持一致
2. SELECT 字段集合与业务含义是否一致
3. 过滤条件（尤其分区过滤）是否保留
4. JOIN 关系是否从 1:N 误改为 N:N
5. migrate 模式下函数返回类型是否一致

若校验失败或存在高风险不确定项，停止自动提交并请求人工确认。

### Step 5: 影响分析

默认执行：
- 调用 MCP `search_lineage_downstream` 查询下游依赖
- 输出下游表/任务、影响等级、建议验证顺序

可选：
- 用户确认后，执行 `register_lineage(full_refresh=true)` 刷新血缘

降级策略：
- 若 MCP 不可用或报错，标记“影响分析已跳过（不阻塞重构）”

### Step 6: 输出交付物

必须包含：
1. 重构后 SQL（含变更注释和规则 ID）
2. 重构报告（改写摘要、风险、等价性与影响分析）
3. A/B 验证 SQL（行数、主键唯一性、核心指标一致性）

模板入口：
- SQL 模板: [assets/output-template.sql](assets/output-template.sql)
- 报告模板: [assets/output-report.md](assets/output-report.md)

## 重构模式引用

仅在需要时加载对应文档，避免一次性加载过多上下文：
- 结构/性能/合规规则: [references/refactoring-patterns.md](references/refactoring-patterns.md)
- Hive→Impala 迁移规则: [references/engine-migration.md](references/engine-migration.md)

## 快捷命令

```bash
/refactor-sql
/refactor-sql --scope=code
/refactor-sql --scope=perf
/refactor-sql --scope=compliance
/refactor-sql --scope=migrate --target=impala
/refactor-sql --no-impact
/refactor-sql --dry-run
```

## 交互确认规则

以下场景必须主动询问用户：
1. 检测到高风险改写（语义可能变化）
2. `migrate` 模式未明确 `--target`
3. 影响分析发现关键下游（核心报表/对外接口）
4. 发现无法自动修复的规则冲突

推荐提问模板：
- “检测到高风险改写 `{rule_id}`，是否继续自动应用？”
- “检测到 `{n}` 个关键下游任务，是否先输出验证 SQL 再落盘？”

## 质量门禁

交付前至少满足：
1. 报告中列出全部应用规则 ID
2. 报告中给出至少 3 组 A/B 校验 SQL
3. 未通过等价性校验时，不输出“可直接上线”结论
