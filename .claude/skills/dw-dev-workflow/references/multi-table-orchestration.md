# 多表编排规范 (Multi-Table Orchestration)

> **本文件是多表编排的唯一真源 (Single Source of Truth)**。
> `dw-requirement-triage`、`generate-etl-sql` 等下游 skill 引用本文件，不重复定义 Task Registry 字段、状态机或 DAG 执行语义。

---

## 1. 模式判定

Phase 1（dw-requirement-triage）Step 8.1 输出后，检查任务数量：

| 条件 | 模式 | 后续行为 |
|------|------|---------|
| 任务数 = 1 且 Step 8.4 未触发下沉 | 单表模式 | 直接进入 Phase 2→3→4→5 |
| 任务数 > 1 | 多表模式 | 创建 plan 文件，进入 DAG 调度；Step 8.3 公共层分析后可能新增任务 |
| Step 8.4 触发 dm 下沉 | 单表→多表升级 | 单表 da 拆为 dm+da 两个任务，创建 plan |
| Phase 4 中用户要求拆表 | 动态升级 | 单表→多表，见 §6 |
| Phase 4 中复杂度评分 >= 60 | 动态升级 | 同上，由 Step 2.1 主动触发，见 §6 |

---

## 2. Plan 文件格式

路径: `docs/wip/plan-{project_name}.md`

```yaml
---
project_name: loan_overdue_report
status: in_progress        # in_progress / completed / failed
created_at: 2026-03-02
updated_at: 2026-03-02
---
```

### DAG 图（ASCII）

```
task-1 (dmm_sac_overdue_dtl) ──┐
                                ├──→ task-3 (da_sac_overdue_rpt)
task-2 (dmm_sac_overdue_prod) ─┘
```

### Task Registry

| task_id | target_table | layer | depends_on | status | phase | req_file | created_by |
|---------|-------------|-------|------------|--------|-------|----------|------------|
| task-1 | ph_sac_dmm.dmm_sac_overdue_dtl | dm | (none) | pending | - | req-dmm_sac_overdue_dtl.md | phase1 |
| task-2 | ph_sac_dmm.dmm_sac_overdue_prod | dm | (none) | pending | - | req-dmm_sac_overdue_prod.md | phase1 |
| task-3 | ph_sac_da.da_sac_overdue_rpt | da | task-1,task-2 | pending | - | req-da_sac_overdue_rpt.md | phase1 |

---

## 3. Task Registry 字段定义

| 字段 | 类型 | 取值 | 说明 |
|------|------|------|------|
| `task_id` | string | `task-1`, `task-2`, ... | plan 内唯一标识 |
| `target_table` | string | `ph_sac_dmm.dmm_sac_overdue_dtl` | 目标表（含物理库名，如 `schema.table`） |
| `layer` | enum | `dm`, `da`, `dws`, `dwd`, `ods` | 数仓分层 |
| `depends_on` | string[] | `task-1,task-2` 或空 | 依赖的任务 ID 列表。Markdown 表格中逗号分隔，JSON 中用数组 `["task-1","task-2"]` |
| `status` | enum | 见 §4 状态机 | 当前状态 |
| `phase` | string | `phase4_etl:step2.5` | 最新完成/暂停的阶段 |
| `req_file` | string | `req-dmm_sac_overdue_dtl.md` | 关联需求文件 |
| `created_by` | enum | `phase1`, `user`, `dynamic` | 来源：Phase 1 分解 / 用户手动 / Phase 4 动态分解 |

### Req 文件扩展字段（多表模式）

单表 req 文件在 front matter 中增加：

```yaml
plan: plan-loan_overdue_report.md
task_id: task-1
phase_progress: phase4_etl:step2.5   # 暂停断点（仅 blocked 时有值）
```

---

## 4. 任务状态机

```
pending ──→ ready ──→ in_progress ──→ completed
              ↑           │
              │           ↓
              └── blocked ←┘ (动态分解触发)
                    │
                    ↓
              (依赖任务完成后) → ready
```

| 状态 | 含义 | 进入条件 |
|------|------|---------|
| `pending` | 初始态，等待依赖完成或调度 | plan 创建时所有任务默认（含有依赖的任务） |
| `ready` | 所有 depends_on 均 completed（或无依赖） | DAG_LOOP 步骤 1 扫描自动标记 |
| `in_progress` | 正在执行 Phase 2→5 | DAG_LOOP 步骤 3 取出执行 |
| `blocked` | 被动态分解阻塞（仅 §6 触发） | Phase 4 中用户要求拆表，当前任务挂起 |
| `completed` | 所有交付物已生成 | Phase 5 QA 完成 |

---

## 5. DAG 执行循环

```
DAG_LOOP:
  1. 扫描 plan，将所有 depends_on 均 completed 的 pending 任务标记为 ready
  2. 从 ready 集合中取 task_id 最小者
  3. 标记该任务 status=in_progress，执行 Phase 2→3→4→[4.5]→5
  4. 该任务完成 → 更新该任务 status=completed，回到 1
  5. 所有任务 completed → plan.status=completed，输出汇总

  异常处理:
  - 无 ready 任务但有未完成任务 → 循环依赖，报错让用户修正 plan
  - Phase 4 中用户请求拆分 → 触发动态分解（见 §6）
```

### Checkpoint 输出（每个任务完成后）

```
═══════════════════════════════════════════════════════════════
 📊 进度: [2/3] task-2 已完成
═══════════════════════════════════════════════════════════════
| task_id | target_table             | status      |
|---------|--------------------------|-------------|
| task-1  | dmm_sac_overdue_dtl      | ✅ completed |
| task-2  | dmm_sac_overdue_prod     | ✅ completed |
| task-3  | da_sac_overdue_rpt       | ⏳ ready     |
═══════════════════════════════════════════════════════════════
下一步: 执行 task-3
```

---

## 6. 动态任务分解 (Dynamic Decomposition)

### 触发条件

以下任一条件满足时触发动态分解:

1. **用户主动请求**: 用户在 Phase 4 过程中明确要求拆表（如"这里需要先建一张中间表"、"这部分逻辑太复杂，拆成两步"）
2. **复杂度评估驱动**: `generate-etl-sql` Step 2.1 复杂度评分 >= 60，AI 建议拆分且用户确认（见 [input-analysis.md](../../generate-etl-sql/references/input-analysis.md) Step 2.1）

### 流程

```
用户请求拆表
    ↓
1. 暂停当前任务
   • 保存进度到 req 文件的 phase_progress
    ↓
2. 生成"动态分解请求"
   • 由 dw-dev-workflow 按统一编排规则创建/更新 plan 并分配 task_id
    ↓
3. 创建新任务 req 文件
   • docs/wip/req-{new_table}.md
   • plan / task_id 由编排层回填
    ↓
4. 执行新任务的 Phase 2→3→4→5
    ↓
5. 恢复原任务
   • 按 plan 最新状态解除阻塞
   • 重新加载 req 文件和源表列表
   • 从暂停点（phase_progress）继续
```

### 动态分解请求最小字段

```json
{
  "current_task": "da_sac_overdue_rpt",
  "split_reason": "当前任务需要先产出中间聚合表",
  "new_tasks": [
    {
      "target_table": "ph_sac_dmm.dmm_sac_overdue_agg",
      "layer": "dm",
      "depends_on": []
    }
  ],
  "resume_from": "phase4_etl:step2.5"
}
```

### 单表升级为多表

当此前为单表模式时，动态分解自动升级：
- 创建 `docs/wip/plan-{project_name}.md`
- 当前任务追溯为 `task-1`（`created_by=phase1`）
- 新任务为 `task-2`（`created_by=dynamic`），`task-1.depends_on += task-2`

---

## 7. 任务分解规则（Phase 1 输出）

由 `dw-requirement-triage` Step 8.2 执行，按优先级排序：

| 优先级 | 规则 | 说明 | 示例 |
|--------|------|------|------|
| 1 | **粒度拆分** | 不同粒度 → 不同表/任务 | 明细(loan_id) + 日聚合(product_code, stat_date) → 2 tasks |
| 2 | **分层拆分** | dm 中间 + da 应用 → 2 任务，da 依赖 dm | dm 指标宽表 + da 报表导出 → task-1(dm) → task-2(da) |
| 3 | **主题拆分** | 不同业务主题共享源表 → 并行任务 | 放款统计 + 逾期统计 → 并行 task-1, task-2 |
| 4 | **依赖推断** | 任务 B 的源表 = 任务 A 的目标表 → A→B | 自动添加依赖边 |

### 输出契约（供编排层消费）

Phase 1 输出任务草案列表，最少包含：

| 字段 | 说明 |
|------|------|
| `target_table` | 目标表名 |
| `layer` | 分层 |
| `depends_on` | 依赖（可空） |
| `split_reason` | 拆分原因 |

- 在 `dw-dev-workflow` 中执行时：由 `dw-dev-workflow` 落盘 plan 并分配 `task_id`
- 独立执行 `dw-requirement-triage` 时：输出建议 DAG，标注为"草案"

---

## 8. 跨会话恢复 (Cross-Session Recovery)

用户中断后恢复时，检测活跃 plan：

```
1. 扫描 docs/wip/plan-*.md
2. 找到 status=in_progress 的 plan
3. 展示当前进度（Task Registry 状态表）
4. 用户确认是否继续
5. 定位最近的 in_progress/blocked 任务
6. 读取其 req 文件的 phase_progress
7. 从断点恢复执行
```

---

## 9. 各 Skill 职责边界

| Skill | 编排职责 | 引用本文件的场景 |
|-------|---------|----------------|
| **dw-dev-workflow** | 主控：plan 创建/更新、task_id 分配、DAG 调度、状态流转 | 全部 §1-§8 |
| **dw-requirement-triage** | Phase 1 输出任务草案，不管理 plan 文件 | §7 分解规则、§3 字段定义（只读） |
| **generate-etl-sql** | Phase 4 触发动态分解请求，暂停/恢复当前任务 | §6 动态分解、§3 字段定义（只读） |
