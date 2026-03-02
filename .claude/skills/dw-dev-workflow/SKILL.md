---
name: dw-dev-workflow
description: 数仓开发全流程工作流。串联 dw-requirement-triage → search-hive-metadata → generate-standard-ddl → generate-etl-sql → generate-qa-suite 五个 skill，实现从需求到上线的端到端自动化。使用场景：(1) 收到业务需求文档后启动完整开发流程 (2) 需要一站式生成 DDL、ETL、测试脚本
---

# 数仓开发工作流 (DW Dev Workflow)

一站式串联数仓开发全流程，从需求拆解到测试套件生成，实现端到端自动化。

## 流程概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           数仓开发工作流                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   需求文档                                                                   │
│       │                                                                     │
│       ▼                                                                     │
│   ┌───────────────────────┐                                                 │
│   │ Phase 1: 需求拆解      │  ← dw-requirement-triage                       │
│   │ • 识别数仓需求         │                                                 │
│   │ • 提取指标/维度        │                                                 │
│   │ • 确定分层/引擎        │                                                 │
│   │ • 多表复杂度检测       │  ← Step 8.1/8.2 (新增)                         │
│   └───────────┬───────────┘                                                 │
│               │                                                             │
│       ┌───────┴───────┐                                                     │
│       ▼               ▼                                                     │
│   [单表模式]      [多表模式]                                                 │
│   (无 plan)    (创建 plan 文件                                               │
│       │         + DAG 循环执行)                                              │
│       │               │                                                     │
│       ▼               ▼                                                     │
│   ┌───────────────────────┐                                                 │
│   │ Phase 2: 复用检索      │  ← search-hive-metadata                        │
│   │ • 指标库查询           │    (MCP Server)                                │
│   │ • 现有表搜索           │                                                 │
│   │ • 粒度匹配判断         │                                                 │
│   │ • 词根查询             │                                                 │
│   └───────────┬───────────┘                                                 │
│               │                                                             │
│       ┌───────┴───────┐                                                     │
│       ▼               ▼                                                     │
│   [复用现有]      [需要开发]                                                 │
│       │               │                                                     │
│   直接 SELECT    ┌────┴────┐                                                │
│                  ▼         │                                                │
│   ┌───────────────────────┐│                                                │
│   │ Phase 3: 模型设计      ││ ← generate-standard-ddl                       │
│   │ • 建模决策(新建/扩列)  ││                                                │
│   │ • 字段命名标准化       ││                                                │
│   │ • 生成 DDL            ││                                                │
│   └───────────┬───────────┘│                                                │
│               │            │                                                │
│               ▼            │                                                │
│   ┌───────────────────────┐│                                                │
│   │ Phase 4: ETL 开发      ││ ← generate-etl-sql                            │
│   │ • 源表分析             ││                                                │
│   │ • 映射逻辑构建         ││                                                │
│   │ • 生成 INSERT SQL      ││                                                │
│   │ • 指标入库             ││                                                │
│   └───────────┬───────────┘│                                                │
│               │            │                                                │
│               ▼            │                                                │
│   ┌───────────────────────┐│                                                │
│   │ Phase 4.5: SQL 审查    ││ ← review-sql (可选)                           │
│   │ • 代码规范检查         ││   --review 参数触发                            │
│   │ • 输出审查报告         ││                                                │
│   └───────────┬───────────┘│                                                │
│               │            │                                                │
│               ▼            │                                                │
│   ┌───────────────────────┐│                                                │
│   │ Phase 5: 测试套件      ││ ← generate-qa-suite                           │
│   │ • 冒烟测试 SQL         ││                                                │
│   │ • DQC 规则             ││                                                │
│   │ • Doris 性能分析       ││                                                │
│   └───────────┬───────────┘│                                                │
│               │            │                                                │
│               ▼            │                                                │
│         [交付物汇总]◄──────┘                                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 使用方式

### 方式一：完整流程

用户提供需求文档或描述后，系统自动串联执行全部 5 个阶段：

```
用户: 帮我开发一个按日+产品统计放款金额的报表

助手:
1. [Phase 1] 需求拆解 → 识别为聚合统计需求，指标=放款金额，维度=日期+产品
2. [Phase 2] 复用检索 → 搜索指标库，检查是否已有放款金额指标...
3. [Phase 3] 模型设计 → 生成 DDL...
4. [Phase 4] ETL 开发 → 生成 INSERT SQL...
5. [Phase 5] 测试套件 → 生成冒烟测试和 DQC 规则...
```

### 方式二：阶段执行

用户可指定从某个阶段开始，或只执行特定阶段：

```
用户: 我已经有 DDL 了，帮我生成 ETL 和测试脚本
助手: [从 Phase 4 开始，跳过 Phase 1-3]
```

**前置数据校验**: 中途进入时，必须先检查跳过阶段的产出物是否已就绪，缺失则提示用户补充：

| 起始阶段 | 必需的前置数据 | 校验方式 |
|---------|--------------|---------|
| `--from=ddl` (Phase 3) | 需求字段列表、建议分层/引擎 | 优先扫描 `docs/wip/` 下 `status: wip` 的需求文件；未找到则询问用户提供需求描述或指标列表 |
| `--from=etl` (Phase 4) | 目标表 DDL + 需求清单（含指标、维度、业务逻辑描述） | 1. 扫描 `docs/wip/` 下需求文件，列出供用户选择 2. 检查用户是否提供了 DDL 文件路径或内容 3. 缺失项要求补充 |
| `--from=qa` (Phase 5) | ETL SQL + 目标表 DDL | 检查用户是否提供了 ETL 和 DDL 文件路径，未提供则要求补充 |

未通过校验时，列出缺失项并询问用户提供，**不自动跳过**。

### 方式三：交互式确认

每个阶段完成后等待用户确认再继续：

```
用户: 逐步帮我开发，每步确认
助手: [Phase 1 完成] 请确认需求拆解结果是否正确？
用户: 确认
助手: [Phase 2 完成] 发现指标库已有 "放款金额"，是否直接复用？
用户: 复用
助手: 已复用现有指标，无需继续开发。
```

---

## 多表模式 (Multi-Table Mode)

当需求涉及多张表（链式依赖、扇出型、混合 DAG）时，工作流自动从单表线性模式升级为多表 DAG 模式。单表需求不受影响。

> **规范详情**: Task Registry 字段定义、状态机、DAG 执行循环、动态分解、跨会话恢复等完整规范见 [references/multi-table-orchestration.md](references/multi-table-orchestration.md)。

### 快速参考

- **模式检测**: Phase 1 Step 8.1 输出 → 任务数=1 走单表，>1 创建 plan 进入 DAG
- **Plan 文件**: `docs/wip/plan-{project_name}.md`（YAML front matter + DAG 图 + Task Registry）
- **DAG 执行**: 扫描依赖 → 取 ready 中最小 task_id → 执行 Phase 2→3→4→[4.5]→5 → 更新 plan → 循环
- **动态分解**: Phase 4 中用户要求拆表 → 暂停 + 分解请求 → 新任务完成后恢复
- **跨会话恢复**: 扫描 `docs/wip/plan-*.md` → 展示进度 → 从 `phase_progress` 断点继续

---

## 阶段详解

### Phase 1: 需求拆解 (dw-requirement-triage)

**触发条件**: 用户提供需求文档或业务描述

**执行内容**:
1. 扫描文档，识别数仓需求 vs 应用需求
2. 提取指标（SUM/COUNT/AVG 等聚合）和维度（GROUP BY 字段）
3. 判断时间粒度（日/周/月）
4. 建议分层（dm/da）和引擎（Hive/Impala/Doris）
5. 识别数据粒度（从 TBLPROPERTIES 或表注释 `[粒度：...]` 获取）

**输出**:
```yaml
需求名称: 放款产品日报表
指标:
  - 当日放款金额 (SUM)
  - 当日放款笔数 (COUNT)
维度:
  - 产品编码
  - 数据日期
时间粒度: 日
建议分层: dm
建议引擎: Hive (T+1)
```

**持久化**: 将结构化需求清单写入 `docs/wip/req-{table_name}.md`（支持跨会话恢复）

**用户确认点**: 需求拆解结果是否正确

---

### Phase 2: 复用检索 (search-hive-metadata)

**触发条件**: Phase 1 完成

**执行内容**:
1. **指标复用检查**: 调用 `search_existing_indicators` 查询指标库
   - 找到 → 展示口径，询问是否复用
   - 未找到 → 继续下一步
2. **现有表搜索**: 调用 `search_table` 和 `search_by_comment` 搜索同主题表
3. **多源消歧**（当字段出现在多张表时）:
   - 应用综合评分策略：口径(40) > 粒度(30) > 分层(20) > 覆盖(10)
   - 输出候选表排名及评分明细
   - 分差 ≥ 10 自动选择，分差 < 10 询问用户
4. **粒度匹配判断**:
   - 优先从 TBLPROPERTIES 获取粒度
   - 若无，从表注释 `[粒度：字段1，字段2]` 解析
   - 比对新需求维度与现有表粒度
5. **词根查询**: 调用 `search_word_root` 获取标准词根

**决策分支**:
```
指标已存在 + 口径一致  →  直接复用，流程结束
指标不存在 OR 口径不符  →  继续 Phase 3
现有表粒度匹配  →  Phase 3 走扩列路径
现有表粒度不匹配  →  Phase 3 走新建路径
多源候选分数接近  →  询问用户选择数据源
```

**用户确认点**:
- 是否复用现有指标
- 多源候选表选择（分差 < 10 时）
- 扩列还是新建表

---

### Phase 3: 模型设计 (generate-standard-ddl)

**触发条件**: Phase 2 决定需要开发

**执行内容**:
1. 确定分层和表名（命名规范：`dmm_sac_{主题}_{粒度}`）
2. 建模决策：
   - **CASE A (扩列)**: 现有表粒度匹配 → `ALTER TABLE ADD COLUMN`
   - **CASE B (新建)**: 无匹配表 → `CREATE TABLE`
   - **CASE C (冲突)**: 粒度匹配但业务跨度大 → 询问用户
3. 字段命名：查词根表，组装标准字段名
4. 生成 DDL（含 COMMENT、TBLPROPERTIES、逻辑主键）

**输出**: 完整的 CREATE TABLE 或 ALTER TABLE 语句

**用户确认点**: DDL 是否符合预期

---

### Phase 4: ETL 开发 (generate-etl-sql)

**触发条件**: Phase 3 完成

**执行内容**:
1. **加载需求上下文**: 从当前会话上下文或 `docs/wip/req-{table_name}.md` 读取需求信息
2. 解析源表 Schema（从 Phase 2 获取）
3. **生成字段映射草案**: 结合需求清单 + 源表 Schema + 目标表 DDL，自动生成映射草案
4. **用户确认/补全映射**: 逐项展示映射，用户确认或补全计算逻辑，确认无遗漏后继续
5. **询问生成模式**: 是否需要生成历史数据初始化脚本？
   - **默认**: 仅生成增量脚本（T+1 日常调度）
   - **可选**: 同时生成增量 + 初始化脚本（新表上线回刷）
6. 分析加工模式（简单聚合/多表关联/窗口计算/分组集）
7. **逻辑流程确认（Step 2.5）**: 多表关联(3+表)/窗口计算/分组集/混合模式时，**必须**先输出伪代码（数据流 + CTE 拆解 + 自检清单），等待用户确认后再生成 SQL。仅简单聚合可跳过
8. 构建 SQL 结构（CTE 拆解复杂逻辑）
9. 生成 INSERT OVERWRITE SQL（适配目标引擎语法）
   - 增量模式：`WHERE stat_date = '${stat_date}'`，静态分区
   - 初始化模式：`WHERE stat_date BETWEEN '${start_date}' AND '${end_date}'`，动态分区
10. **指标入库检查**: 识别新指标，询问是否注册到指标库
11. **血缘注册**: 自动提取并注册表级/字段级血缘关系

**输出**:
- **增量模式**: `{table_name}_etl.sql`
- **初始化模式**: `{table_name}_etl.sql` + `{table_name}_init.sql`

**用户确认点**:
- 字段映射草案是否正确、是否有遗漏（Step 0 确认）
- 是否需要生成初始化脚本（新表上线场景）
- **逻辑流程/伪代码**是否正确（Step 2.5 确认，复杂模式必须）
- ETL 逻辑是否正确
- 新指标是否入库

**自动执行（无需确认）**:
- 血缘注册（调用 `register_lineage`）

---

### Phase 4.5: SQL 审查 (review-sql) — 可选

**触发条件**: Phase 4 完成 + 用户使用 `--review` 参数或手动请求审查

**执行内容**:
1. 将 Phase 4 生成的 ETL SQL 作为输入
2. 执行 review-sql 审查流程（DDL 规则 + ETL 规则 + 通用规则）
3. 输出审查报告（FATAL/ERROR/WARN/INFO 分级）
4. 如有 FATAL 项，阻断流程并提示修复

**决策分支**:
```
审查结果无 FATAL → 继续 Phase 5
审查结果有 FATAL → 提示修复，修复后复查（最多 3 轮）
  ├─ 复查通过 → 继续 Phase 5
  └─ 达到 3 轮仍有 FATAL → 停止循环，列出未解决项，询问用户：
       (a) 手动修复后重新提交
       (b) 跳过审查继续 Phase 5（记录为已知风险）
用户跳过 → 直接进入 Phase 5
```

**用户确认点**: 是否接受审查结果并继续

**默认行为**: 跳过（不加 `--review` 时不执行此阶段）

---

### Phase 5: 测试套件 (generate-qa-suite)

**触发条件**: Phase 4（或 Phase 4.5）完成

**执行内容**:
1. 解析 ETL 上下文（源表、目标表、主键、字段类型）
2. 生成冒烟测试（行数、主键唯一性、NULL 检查、样本抽查）
3. 生成 DQC 规则（完整性、唯一性、有效性、一致性、波动率）
4. Doris 专项：生成 EXPLAIN 分析请求

**输出**: 完整的 QA Suite（冒烟测试 + DQC 规则 + 性能分析）

**用户确认点**: 测试覆盖是否充分

---

## 交付物汇总

### 单表模式

流程完成后，输出以下交付物：

```
📦 交付物
├── 📄 docs/wip/req-{table_name}.md  ← Phase 1 输出（需求清单 + 已确认映射，status: done）
├── 📄 {table_name}_ddl.sql          ← Phase 3 输出
├── 📄 {table_name}_etl.sql          ← Phase 4 输出（日常增量，含血缘注释）
├── 📄 {table_name}_init.sql         ← Phase 4 输出（可选，历史回刷）
├── 📄 {table_name}_qa.sql           ← Phase 5 输出
└── 📊 元数据更新
    ├── 指标库: indicator_registry (新指标)
    └── 血缘库: data_lineage, column_lineage (自动采集)
```

### 多表模式

所有任务完成后，输出汇总交付物：

```
📦 交付物 (多表模式)
├── 📄 docs/wip/plan-{project_name}.md  ← Plan 文件（status: completed）
├── 📂 task-1: {table_name_1}
│   ├── 📄 docs/wip/req-{table_name_1}.md  (status: done)
│   ├── 📄 {table_name_1}_ddl.sql
│   ├── 📄 {table_name_1}_etl.sql
│   ├── 📄 {table_name_1}_init.sql         (可选)
│   └── 📄 {table_name_1}_qa.sql
├── 📂 task-2: {table_name_2}
│   ├── 📄 docs/wip/req-{table_name_2}.md  (status: done)
│   ├── 📄 {table_name_2}_ddl.sql
│   ├── 📄 {table_name_2}_etl.sql
│   └── 📄 {table_name_2}_qa.sql
├── ...
└── 📊 元数据更新（统一注册）
    ├── 指标库: indicator_registry (所有任务的新指标)
    └── 血缘库: data_lineage, column_lineage (所有任务的血缘)
```

---

## 状态机

工作流状态转换（含单表/多表分支）：

```
                    ┌──────────────────┐
                    │      START       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ PHASE_1_TRIAGE   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ COMPLEXITY_CHECK │
                    └────────┬─────────┘
                    ┌────────┴────────┐
                    │                 │
                    ▼                 ▼
            ┌──────────────┐  ┌──────────────────┐
            │  单表模式     │  │  多表模式          │
            │  (无 plan)   │  │  CREATE_PLAN      │
            └──────┬───────┘  └────────┬─────────┘
                   │                   │
                   ▼                   ▼
          ┌──────────────────┐  ┌──────────────────┐
  ┌───────│ PHASE_2_SEARCH   │  │    DAG_LOOP      │
  │       └──────────────────┘  │ ┌──────────────┐ │
  │ 复用          │ 开发        │ │ 取 ready 任务 │ │
  ▼               ▼             │ │ 执行 Ph2→5  │ │
┌──────┐  ┌────────────────┐   │ │ 更新 plan   │ │
│DONE  │  │ PHASE_3_DDL    │   │ │ ↻ 直到全部   │ │
│(复用) │  └────────┬───────┘   │ │   completed │ │
└──────┘           │           │ └──────────────┘ │
                   ▼           └────────┬─────────┘
          ┌──────────────────┐          │
          │ PHASE_4_ETL      │          ▼
          └────────┬─────────┘  ┌──────────────────┐
                   │            │  PLAN_COMPLETE    │
           ┌───────┴───────┐   │  (多表汇总交付)    │
           │ --review?     │   └──────────────────┘
           ▼               ▼
  ┌──────────────┐   (跳过)
  │PHASE_4.5     │     │
  │_REVIEW (可选) │     │
  └──────┬───────┘     │
         │             │
         └──────┬──────┘
                ▼
          ┌──────────────────┐
          │ PHASE_5_QA       │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │    COMPLETE      │
          │   (单表交付)      │
          └──────────────────┘
```

---

## 项目记忆 (Project Memory)

工作流与 MEMORY.md（auto memory）联动，实现跨会话持续积累。

### 读取时机

| 阶段 | 读取内容 | 用途 |
|------|---------|------|
| Phase 2 开始前 | MEMORY.md 表清单 | 补充 MCP 搜索结果，识别近期新建但元数据未同步的表 |
| Phase 2 开始前 | pitfalls.md 源表特性 | 了解源表已知问题（NULL、负值、分区特殊性） |
| Phase 3 开始前 | MEMORY.md 决策日志 | 参考历史决策保持一致性 |
| Phase 4 开始前 | pitfalls.md ETL 错误 | 注入到 Step 2.5 自检清单 |
| 全流程 | MEMORY.md 用户偏好 | 沿用用户已确认的偏好，减少重复提问 |

### 写入时机

每个 Phase 完成后，**自动更新 MEMORY.md**：

| 阶段完成 | 写入内容 | 目标位置 |
|---------|---------|---------|
| Phase 1 | 结构化需求清单 | `docs/wip/req-{table_name}.md` |
| Phase 2（决定复用） | 记录复用决策 | 决策日志 |
| Phase 3（DDL 生成） | 表名、粒度、操作类型（新建/扩列） | 表清单 + 决策日志 |
| Phase 4（ETL 生成） | 新注册的指标 | 决策日志 |
| Phase 5 | 无 | — |
| 用户指出 ETL 问题时 | 问题描述、原因、修正方式 | pitfalls.md |
| 首次运行时 | 采集用户偏好（存储格式、确认模式等） | 用户偏好 |

### 记忆文件位置

- `MEMORY.md` — auto memory 主文件（表清单、决策日志、用户偏好）
- `pitfalls.md` — 踩坑记录（源表特性、ETL 错误、DQC 真实缺陷）

---

## 上下文传递

各阶段之间的数据传递：

| 源阶段 | 目标阶段 | 传递内容 |
|--------|----------|---------|
| Phase 1 → Phase 2 | 指标名称列表、维度列表、建议分层/引擎 |
| Phase 1 → `docs/wip/` | 结构化需求清单持久化（跨会话载体） |
| Phase 1 → Phase 4 | 需求清单（指标、维度、业务逻辑描述、数据来源）— 同会话通过上下文传递，跨会话通过 `docs/wip/req-{name}.md` 恢复 |
| Phase 2 → Phase 3 | 指标复用结果、现有表搜索结果、粒度匹配判断、词根查询结果 |
| Phase 3 → Phase 4 | 目标表 DDL（含主键、字段列表、COMMENT） |
| Phase 3 → `docs/wip/` | 更新需求文件中的 `target_table` 为确定表名 |
| Phase 4 → Phase 4.5 | ETL SQL（可选，--review 时传递） |
| Phase 4 → `docs/wip/` | 回写已确认的字段映射到需求文件；**任何需求变更（新增/移除源表、调整指标口径、修改维度、补充业务逻辑等）同步更新对应章节** |
| Phase 4 / 4.5 → Phase 5 | ETL SQL（含源表、加工逻辑）、目标表 DDL |
| Phase 4 → 指标库 | 新指标注册请求 |
| Phase 4 → 血缘库 | 表级/字段级血缘关系（自动注册） |
| Phase 3 → MEMORY.md | 表清单更新（新建/扩列） |
| Phase 4 → MEMORY.md | 决策日志更新（指标入库） |
| **任意阶段** → `docs/wip/` | **需求变更即时同步**：任何阶段发生需求变更（新增/移除源表、调整指标口径、修改维度、补充业务逻辑等），必须立即更新 `docs/wip/req-{table_name}.md` 的对应章节，确保 wip 文件始终反映最新需求状态 |
| Phase 5 → `docs/wip/` | 更新需求文件状态为 `status: done` |
| 用户反馈 → pitfalls.md | 踩坑记录（源表特性、ETL 错误） |
| plan 文件 → DAG_LOOP | 拓扑排序、任务状态、依赖关系 |
| 已完成任务 N 的 DDL → 依赖任务 M 的 Phase 2 | 作为源表元数据（虚拟元数据，表可能尚未在 metastore 中创建） |
| 动态分解 (Phase 4) → plan 文件 | 新任务节点 + 依赖边 + 当前任务阻塞标记 |

---

## 异常处理

| 场景 | 处理方式 |
|------|---------|
| Phase 1 无法识别数仓需求 | 输出"非数仓需求"，流程终止 |
| Phase 2 指标口径冲突 | 询问用户确认：复用/重新开发 |
| Phase 2 粒度无法确定 | 标记"粒度待确认"，询问用户 |
| Phase 3 建模冲突 (CASE C) | 询问用户：扩列/新建/其他 |
| Phase 4 映射关系不明确 | 展示映射草案，要求用户逐项确认/补全，确认无遗漏后继续 |
| Phase 5 无法匹配 DQC 规则 | 生成通用规则 + 标记待人工补充 |

---

## MCP 工具依赖

本工作流依赖 `search-hive-metadata` MCP Server 提供的工具：

| 工具 | 用途 | 使用阶段 |
|------|------|---------|
| `search_existing_indicators` | 指标库查询，复用判断 | Phase 2 |
| `search_table` | 按表名搜索现有表 | Phase 2, 3 |
| `search_by_comment` | 按业务术语搜索表/字段 | Phase 2 |
| `get_table_detail` | 获取表详情（含粒度信息） | Phase 2, 3 |
| `search_word_root` | 查询词根，标准化字段命名 | Phase 3 |
| `register_indicator` | 新指标入库 | Phase 4 |
| `register_lineage` | 注册表级/字段级血缘 | Phase 4 |
| `search_lineage_upstream` | 查询上游依赖（我依赖谁） | Phase 2, 影响分析 |
| `search_lineage_downstream` | 查询下游影响（谁依赖我） | 影响分析, 变更评估 |

### MCP 不可用时的降级策略

当 MCP Server 连接失败或超时时，按以下策略降级，**不阻塞工作流**：

| 工具类型 | 降级方式 | 影响 |
|---------|---------|------|
| **查询类** (`search_table`, `search_by_comment`, `get_table_detail`, `list_columns`, `search_word_root`, `search_existing_indicators`, `search_lineage_*`) | 告知用户 MCP 不可用，请求用户手动提供所需信息（表名、字段列表、指标口径等），然后继续流程 | 元数据自动补全失效，依赖用户输入 |
| **写入类** (`register_indicator`, `register_lineage`) | 将待注册数据以 JSON 格式输出到脚本头部注释中，标记 `-- [MCP-PENDING]`，提示用户 MCP 恢复后手动补录 | 指标/血缘注册延后，不影响 SQL 产出 |

**检测方式**: 首次调用 MCP 工具时若返回连接错误或超时，即判定为不可用，后续步骤自动切换降级模式，不再重试。

---

## 快捷命令

| 命令 | 说明 |
|------|------|
| `/dw-workflow` | 启动完整流程 |
| `/dw-workflow --from=ddl` | 从 Phase 3 开始（需提供需求描述） |
| `/dw-workflow --from=etl` | 从 Phase 4 开始（需提供 DDL） |
| `/dw-workflow --from=qa` | 从 Phase 5 开始（需提供 ETL + DDL） |
| `/dw-workflow --step` | 逐步执行，每步确认 |
| `/dw-workflow --review` | ETL 生成后执行 SQL 审查（Phase 4.5） |
| `/dw-workflow --dry-run` | 仅分析，不生成文件 |
| `/dw-workflow --plan` | 强制多表模式，前置规划（即使需求看起来是单表） |
| `/dw-workflow --resume` | 从已有 plan 文件恢复执行（扫描 `docs/wip/plan-*.md`） |

---

## 完整示例

完整执行示例已下沉到参考文件，包含：

- 单表模式 5 阶段执行轨迹
- 多表模式触发与编排衔接
- 典型交付物清单

见 [references/workflow-e2e-example.md](references/workflow-e2e-example.md)。

---

## 配置要求

### MCP Server 配置

本工作流需要 `search-hive-metadata` MCP Server 正常运行：

```json
{
  "mcpServers": {
    "hive-metadata": {
      "command": "python",
      "args": ["path/to/mcp_server.py"],
      "env": {
        "MYSQL_HOST": "mysql-host",
        "MYSQL_DATABASE": "hive_metadata",
        "PG_HOST": "pg-host",
        "PG_DATABASE": "indicator_db"
      }
    }
  }
}
```

### 相关 Skill

确保以下 skill 已安装：

- `dw-requirement-triage.skill`
- `generate-standard-ddl.skill`
- `generate-etl-sql.skill`
- `review-sql` (可选，Phase 4.5 审查)
- `generate-qa-suite.skill`

## References

- [references/multi-table-orchestration.md](references/multi-table-orchestration.md) - 多表编排规范（Task Registry、状态机、DAG 执行、动态分解）
- [references/workflow-e2e-example.md](references/workflow-e2e-example.md) - 端到端执行示例（单表/多表）
