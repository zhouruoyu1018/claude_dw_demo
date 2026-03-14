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

### 方式一：完整流程（默认带门控）

用户提供需求文档或描述后，串联执行全部阶段。**每个 Phase 完成后必须停下等待用户确认，确认通过后才进入下一阶段**：

```
用户: 帮我开发一个按日+产品统计放款金额的报表

助手: [Phase 1] 需求拆解完成 → 输出需求清单
  ⏸️ 等待用户确认需求拆解结果
用户: 确认
助手: [Phase 2] 复用检索完成 → 未找到可复用指标，建议新建
  ⏸️ 等待用户确认开发方案
用户: 确认，新建
助手: [Phase 3] 模型设计完成 → 输出 DDL
  ⏸️ 等待用户确认 DDL
用户: 确认
助手: [Phase 4] ETL 开发完成 → 输出 SQL
  ⏸️ 等待用户确认 ETL 逻辑
用户: 确认
助手: [Phase 5] 测试套件完成 → 输出测试脚本
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
| `--from=patch` (Phase 4 patch) | 现有 ETL 脚本路径 + 变更需求描述 | 1. 检查 ETL 文件是否存在 2. 要求用户提供变更需求描述 3. 可选提供 ALTER TABLE DDL（新增字段时需要） |
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

**☑️ 必做清单（按顺序执行，不可跳过）**:
1. ⚠️ **必须**调用 `dw-requirement-triage` skill（禁止自行拆解需求）
2. 将结构化需求清单写入 `docs/wip/req-{table_name}.md`（`status: wip`）
3. 如首次运行，采集用户偏好写入 MEMORY.md

> **⏸️ GATE**: 展示需求清单，等待用户确认拆解结果正确。禁止自动跳转到 Phase 2。

执行细节见 [references/phase-execution-details.md](references/phase-execution-details.md)

---

### Phase 2: 复用检索 (search-hive-metadata)

**触发条件**: Phase 1 用户确认通过

**☑️ 必做清单（按顺序执行，不可跳过）**:
1. 读取 MEMORY.md 表清单（补充 MCP 搜索，识别近期新建但元数据未同步的表）
2. 读取 pitfalls.md（了解源表已知问题）
3. 调用 `search_existing_indicators` 检查指标是否已存在
4. 调用 `search_table` / `search_by_comment` 搜索同主题现有表
5. 多源候选时执行消歧评分（口径40 > 粒度30 > 分层20 > 覆盖10）
6. 粒度匹配判断（优先 TBLPROPERTIES，次选表注释 `[粒度：...]`）
7. 调用 `search_word_root` 获取标准词根（为 Phase 3 准备）
8. 如决定复用现有指标，将复用决策写入 MEMORY.md 决策日志

**决策分支**: 指标复用(结束) / 继续开发(Phase 3) / 扩列 / 新建

> **⏸️ GATE**: 展示复用检索结果，等待用户确认以下决策：
> - 是否复用现有指标（复用则流程结束）
> - 多源候选如何选择（分差 < 10 时必须询问）
> - 扩列还是新建
>
> ❌ 用户未确认前，禁止进入 Phase 3。

执行细节见 [references/phase-execution-details.md](references/phase-execution-details.md)

---

### Phase 3: 模型设计 (generate-standard-ddl)

**触发条件**: Phase 2 用户确认需要开发

**☑️ 必做清单（按顺序执行，不可跳过）**:
1. 读取 MEMORY.md 决策日志（参考历史决策保持一致性）
2. ⚠️ **必须**调用 `generate-standard-ddl` skill（禁止自行生成 DDL）
3. 将表名、粒度、操作类型（新建/扩列）写入 MEMORY.md 表清单 + 决策日志
4. 更新 `docs/wip/req-{table_name}.md` 中的 `target_table` 为确定表名

**输出**: CREATE TABLE 或 ALTER TABLE 语句

> **⏸️ GATE**: 展示 DDL + 字段设计表，等待用户确认 DDL 符合预期。禁止自动跳转到 Phase 4。

执行细节见 [references/phase-execution-details.md](references/phase-execution-details.md)

---

### Phase 4: ETL 开发 (generate-etl-sql)

**触发条件**: Phase 3 用户确认 DDL 通过

**☑️ 必做清单（按顺序执行，不可跳过）**:
1. 读取 pitfalls.md（注入到 Step 2.5 自检清单）
2. ⚠️ **必须**调用 `generate-etl-sql` skill（禁止自行编写 ETL SQL）
   - Phase 3 输出 **CREATE TABLE** → 默认模式（incremental/init）
   - Phase 3 输出 **ALTER TABLE** → `--mode=patch`

**📌 以下动作在 GATE 用户确认通过后执行**:
3. 将新注册的指标写入 MEMORY.md 决策日志
4. 回写已确认的字段映射到 `docs/wip/req-{table_name}.md`；任何需求变更同步更新对应章节
5. 自动调用 `register_lineage` 注册血缘（修改模式使用 `full_refresh=true`，无需用户确认）

**模式详情**:

#### 新建模式（incremental/init）

1. 加载需求上下文（从会话或 `docs/wip/req-{table_name}.md`）
2. 解析源表 Schema → 生成字段映射草案 → 用户确认/补全映射
3. 询问是否需要初始化脚本（默认仅增量）
4. 分析加工模式 → 复杂模式（3+表关联/窗口/分组集）**必须**先输出伪代码等用户确认
5. 生成 INSERT OVERWRITE SQL → 指标入库检查 → 血缘注册

#### 修改模式（patch）

1. 定位现有 ETL 脚本（`sql/hive/etl/{table_name}_etl.sql`，未找到则询问用户）
2. 传入 `generate-etl-sql --mode=patch` → 内部 Step P0~P3 + Step 4~6
3. 输出修改后的完整 ETL 脚本

**输出**:
- 新建: `{table_name}_etl.sql`（+ 可选 `{table_name}_init.sql`）
- 修改: 修改后的 `{table_name}_etl.sql`（含 changelog）

> **⏸️ GATE**: 展示 ETL 产出并等待用户逐项确认：
> - 字段映射是否正确、是否有遗漏
> - 逻辑流程/伪代码是否正确
> - ETL SQL 是否正确
> - 新指标是否入库
>
> ❌ 用户未确认前，禁止进入 Phase 4.5 或 Phase 5。

---

### Phase 4.5: SQL 审查 (review-sql) — 可选

**触发条件**: Phase 4 用户确认通过 + `--review` 参数或用户手动请求审查
**默认行为**: 跳过（不加 `--review` 时不执行此阶段）

**☑️ 必做清单（仅 --review 时执行）**:
1. ⚠️ **必须**调用 `review-sql` skill（禁止自行审查）
2. FATAL 项阻断 → 修复后复查（最多 3 轮，超出则列出未解决项询问用户）
3. 审查通过或用户选择跳过 → 进入 Phase 5

执行细节见 [references/phase-execution-details.md](references/phase-execution-details.md)

---

### Phase 5: 测试套件 (generate-qa-suite)

**触发条件**: Phase 4 用户确认 ETL 通过（或 Phase 4.5 审查通过）

**☑️ 必做清单（按顺序执行，不可跳过）**:
1. ⚠️ **必须**调用 `generate-qa-suite` skill（禁止自行编写测试）

**输出**: 冒烟测试 + DQC 规则 + Doris 性能分析

> **⏸️ GATE**: 展示测试套件，等待用户确认：
> - 测试覆盖是否充分
> - DQC 规则是否符合业务预期
>
> ❌ 用户未确认前，禁止标记工作流完成。

**📌 GATE 确认通过后执行**:
2. 更新 `docs/wip/req-{table_name}.md` 状态为 `status: done`

执行细节见 [references/phase-execution-details.md](references/phase-execution-details.md)

---

## 交付物汇总

### 单表模式

流程完成后，输出以下交付物：

```
📦 交付物
├── 📄 docs/wip/req-{table_name}.md  ← Phase 1 输出（需求清单 + 已确认映射，status: done）
├── 📄 {table_name}_ddl.sql          ← Phase 3 输出
├── 📄 {table_name}_etl.sql          ← Phase 4 输出（新建/修改后的完整脚本，含血缘注释和 changelog）
├── 📄 {table_name}_init.sql         ← Phase 4 输出（可选，历史回刷/同步更新）
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

## 状态机与协作

状态转换概要：START → PHASE_1 → COMPLEXITY_CHECK → [单表/多表] → PHASE_2 → [复用→DONE / 开发→PHASE_3] → PHASE_4 (CREATE→incr / ALTER→patch) → [PHASE_4.5] → PHASE_5 → COMPLETE

详细状态图、项目记忆联动、上下文传递表、异常处理规则见：
[references/state-machine.md](references/state-machine.md)

---

## MCP 工具依赖

本工作流依赖 `search-hive-metadata` MCP Server 提供的工具：

| 工具 | 用途 | 使用阶段 |
|------|------|---------|
| `search_existing_indicators` | 指标库查询，复用判断 | Phase 2 |
| `search_table` | 按表名搜索现有表 | Phase 2, 3 |
| `search_by_comment` | 按业务术语搜索表/字段 | Phase 2 |
| `get_table_detail` | 获取表详情（含粒度信息） | Phase 2, 3 |
| `search_word_root` | 查询词根，标准化字段命名 | Phase 2, 3 |
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
| `/dw-workflow --from=patch` | 从 Phase 4 patch 模式开始（需提供现有 ETL + 变更需求） |
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

- [references/phase-execution-details.md](references/phase-execution-details.md) - Phase 1/2/3/4.5/5 执行详解
- [references/state-machine.md](references/state-machine.md) - 状态机、项目记忆、上下文传递、异常处理
- [references/multi-table-orchestration.md](references/multi-table-orchestration.md) - 多表编排规范（Task Registry、状态机、DAG 执行、动态分解）
- [references/workflow-e2e-example.md](references/workflow-e2e-example.md) - 端到端执行示例（单表/多表）
