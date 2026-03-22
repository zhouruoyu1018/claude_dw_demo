# Req 文件 Schema 规范

> 本文件定义 `docs/wip/req-{table_name}.md` 的 YAML frontmatter 结构（Schema v2）。
> 工作流各阶段按此规范写入 checkpoint，`--from=` 和 `--resume` 按此规范读取恢复点。

---

## Schema 版本

| 版本 | 说明 |
|------|------|
| v1（隐式） | 无 `schema_version` 字段的历史文件，仅有 `status`/`table_name` 等基础字段 |
| **v2**（当前） | 新增 `schema_version`、`phase_completed`、`phase_checkpoints` |

**向后兼容**: `schema_version` 缺失时视为 v1，工作流正常执行但不写入 checkpoint；`--from=` 回退到现有校验逻辑（扫描文件是否存在）。

**无 req 文件场景**: 用户通过 `--from=etl` 等直接提供文件路径进入工作流、未经过 Phase 1 创建 req 文件时，checkpoint 写入**静默跳过**，不影响工作流正常执行。Checkpoint 是增强功能，不是前置依赖。

---

## v2 Frontmatter 完整定义

```yaml
---
schema_version: 2                # 必填，固定值 2
status: wip                      # 必填，wip | done
phase_completed: 0               # 必填，最后完成的阶段编号（0=刚创建，1~5=对应阶段）
table_name: dmm_sac_xxx          # 必填，目标表名（不含库名前缀）
schema: ph_sac_dmm               # 必填，物理库名
layer: dm                        # 必填，逻辑分层 dm | da
engine: hive                     # 必填，主引擎 hive | impala | doris
created: 2026-03-22              # 必填，创建日期
requirement_source: ""           # 可选，需求文档路径
version: ""                      # 可选，需求版本号

phase_checkpoints:
  # ── Phase 1: 需求拆解 ──
  phase1:
    dimensions: []               # 维度字段列表 e.g. [org_code, stat_date]
    granularity: ""              # 粒度描述 e.g. "org_code + stat_date 日粒度"
    target_layer: dm             # dm | da
    target_engine: hive          # hive | impala | doris
    indicator_count: 0           # 指标数量
    complexity_score: 0          # Step 8.1 复杂度评分（0=单表，>0 多表）
    multi_table_mode: false      # 是否触发多表模式
    completed_at: ""             # ISO 8601 时间戳

  # ── Phase 2: 复用检索 ──
  phase2:
    reuse_decision: ""           # reuse | new_build | extend
    matched_indicators: []       # 匹配到的已有指标 ID 列表
    granularity_match: false     # 是否找到粒度匹配的现有表
    candidate_tables: []         # 候选表列表 e.g. [ph_sac_dmm.dmm_sac_xxx]
    word_roots_cached: []        # 已查询的词根缓存，Phase 3 直接使用
    completed_at: ""

  # ── Phase 3: 模型设计 ──
  phase3:
    modeling_case: ""            # A(扩列) | B(新建) | C(冲突)
    ddl_type: ""                 # create | alter
    ddl_path: ""                 # DDL 文件路径 e.g. sql/hive/ddl/xxx_ddl.sql
    field_count: 0               # 字段数量
    partition_fields: []         # 分区字段 e.g. [stat_date]
    word_root_validated: false   # 词根校验是否通过
    completed_at: ""

  # ── Phase 4: ETL 开发（4a→4d 子步骤） ──
  phase4:
    phase4_progress: ""          # 子步骤进度: "" | 4a | 4b | 4c | done
    # ── 4a（映射确认）完成时写入 ──
    mapping_confirmed: false     # 字段映射已确认
    inversion_gaps_found: 0      # Inversion Gate 发现的缺口数
    source_tables: []            # 解析到的源表列表 e.g. [ph_sac_dmm.dmm_sac_xxx]
    # ── 4b（逻辑计划）完成时写入 ──
    etl_mode: ""                 # incremental | patch
    complexity_score: 0          # Step 2.1 复杂度评分（0-100）
    logic_plan_approved: false   # 逻辑计划/伪代码已确认
    logic_plan_snapshot: ""      # 逻辑计划快照（YAML 字符串），含:
                                 #   steps: [{name, input, output, granularity, join_keys}]
                                 #   pattern: "简单聚合|多表关联|窗口计算|分组集|混合"
                                 #   key_decisions: ["LEFT JOIN dim_dept ON dept_psid", ...]
    split_suggested: false       # 是否建议拆分中间表
    # ── 4c（SQL 生成）完成时写入 ──
    etl_path: ""                 # ETL 文件路径 e.g. sql/hive/etl/xxx_etl.sql
    init_path: ""                # 初始化脚本路径（可选，无则为空）
    # ── 4d（元数据注册）完成时写入 ──
    indicators_registered: []    # 已注册指标 ID 列表 e.g. [IDX_001, IDX_002]
    indicator_register_scope: "" # all | partial | none（用户确认的注册范围）
    lineage_registered: false    # 血缘是否已注册
    registration_status: ""      # success | partial_failure | skipped
    review_result: null          # 仅 Phase 4.5 填写 {fatal: 0, error: 0, warn: 0}
    completed_at: ""

  # ── Phase 5: 测试套件 ──（Phase 5 完成时 status 同步改为 done）
  phase5:
    qa_path: ""                  # QA 文件路径 e.g. sql/hive/qa/xxx_qa.sql
    smoke_test_count: 0          # 冒烟测试数量
    dqc_rule_count: 0            # DQC 规则数量
    completed_at: ""
---
```

---

## 字段使用规则

### 写入时机

| 阶段 | 写入字段 | 触发点 |
|------|---------|--------|
| Phase 1 GATE 通过后 | `phase_checkpoints.phase1.*`, `phase_completed: 1` | 用户确认需求拆解 |
| Phase 2 GATE 通过后 | `phase_checkpoints.phase2.*`, `phase_completed: 2` | 用户确认复用决策 |
| Phase 3 GATE 通过后 | `phase_checkpoints.phase3.*`, `phase_completed: 3` | 用户确认 DDL |
| Phase 4a 门禁通过后 | `phase4.mapping_confirmed`, `inversion_gaps_found`, `source_tables`, `phase4_progress: "4a"` | 用户确认映射 |
| Phase 4b 门禁通过后 | `phase4.etl_mode`, `complexity_score`, `logic_plan_approved`, `logic_plan_snapshot`, `split_suggested`, `phase4_progress: "4b"` | 用户确认逻辑计划（或跳过） |
| Phase 4c 门禁通过后 | `phase4.etl_path`, `init_path`, `phase4_progress: "4c"` | 用户确认 SQL |
| Phase 4d 完成后 | `phase4.indicators_registered`, `indicator_register_scope`, `lineage_registered`, `registration_status`, `completed_at`, `phase4_progress: "done"`, `phase_completed: 4` | 注册完成（成功/部分失败/跳过） |
| Phase 5 GATE 通过后 | `phase_checkpoints.phase5.*`, `phase_completed: 5`, `status: done` | 用户确认测试套件 |

### 读取时机

| 场景 | 读取字段 | 用途 |
|------|---------|------|
| `--from=ddl` | `phase_completed >= 2`, `phase2.word_roots_cached` | 跳过 Phase 1-2，复用词根缓存 |
| `--from=etl` | `phase_completed >= 3`, `phase3.ddl_path`, `phase3.ddl_type` | 跳过 Phase 1-3，加载 DDL 上下文 |
| `--from=patch` | `phase4.etl_path`（如有） | 定位现有 ETL 脚本 |
| `--from=qa` | `phase_completed >= 4`, `phase4.etl_path`, `phase3.ddl_path` | 跳过 Phase 1-4，加载 ETL + DDL |
| `--resume`（阶段级） | `phase_completed`, 找到最后完成阶段的下一个 | 断点续跑 |
| `--resume`（Phase 4 内） | `phase4.phase4_progress` | 定位 Phase 4 子步骤断点（4a→4b→4c→4d） |
| Phase 3 开始 | `phase2.word_roots_cached` | 直接使用词根，不重复查询 |
| Phase 4a 开始 | `phase3.ddl_type` | 决定 ETL 模式（incremental vs patch） |
| Phase 4b 开始 | `phase4.mapping_confirmed`, `source_tables` | 复用已确认映射 |
| Phase 4c 开始 | `phase4.logic_plan_snapshot`, `logic_plan_approved` | 复用逻辑计划，不重新生成 |
| Phase 4d 开始 | `phase4.etl_path`, `init_path` | 定位 ETL 文件执行注册 |
| Phase 5 开始 | `phase4.etl_path`, `phase3.ddl_path` | 定位输入文件 |

---

## 示例：完整生命周期

```yaml
# Phase 1 完成后
---
schema_version: 2
status: wip
phase_completed: 1
table_name: dmm_sac_loan_prod_daily
schema: ph_sac_dmm
layer: dm
engine: hive
created: 2026-03-22
phase_checkpoints:
  phase1:
    dimensions: [product_code, stat_date]
    granularity: "product_code + stat_date 日粒度"
    target_layer: dm
    target_engine: hive
    indicator_count: 5
    complexity_score: 0
    multi_table_mode: false
    completed_at: "2026-03-22T10:00:00"
---
```

```yaml
# Phase 5 完成后（全流程结束）
---
schema_version: 2
status: done
phase_completed: 5
table_name: dmm_sac_loan_prod_daily
schema: ph_sac_dmm
layer: dm
engine: hive
created: 2026-03-22
phase_checkpoints:
  phase1:
    dimensions: [product_code, stat_date]
    granularity: "product_code + stat_date 日粒度"
    target_layer: dm
    target_engine: hive
    indicator_count: 5
    complexity_score: 0
    multi_table_mode: false
    completed_at: "2026-03-22T10:00:00"
  phase2:
    reuse_decision: new_build
    matched_indicators: []
    granularity_match: false
    candidate_tables: []
    word_roots_cached: [{word: loan, root: LOAN}, {word: product, root: PROD}]
    completed_at: "2026-03-22T10:15:00"
  phase3:
    modeling_case: B
    ddl_type: create
    ddl_path: sql/hive/ddl/dmm_sac_loan_prod_daily_ddl.sql
    field_count: 12
    partition_fields: [stat_date]
    word_root_validated: true
    completed_at: "2026-03-22T10:30:00"
  phase4:
    phase4_progress: done
    mapping_confirmed: true
    inversion_gaps_found: 1
    source_tables: [ph_sac_dwd.dwd_loan_detail, ph_sac_dim.dim_product]
    etl_mode: incremental
    complexity_score: 35
    logic_plan_approved: true
    logic_plan_snapshot: "steps: [{name: base, input: dwd_loan_detail, output: filtered, granularity: loan_id}, {name: agg_daily, input: base, output: aggregated, granularity: product_code+stat_date, join_keys: [product_code]}]; pattern: 多表关联; key_decisions: [LEFT JOIN dim_product ON product_code]"
    split_suggested: false
    etl_path: sql/hive/etl/dmm_sac_loan_prod_daily_etl.sql
    init_path: ""
    indicators_registered: [IDX_LP_001, IDX_LP_002, IDX_LP_003]
    indicator_register_scope: all
    lineage_registered: true
    registration_status: success
    review_result: null
    completed_at: "2026-03-22T11:00:00"
  phase5:
    qa_path: sql/hive/qa/dmm_sac_loan_prod_daily_qa.sql
    smoke_test_count: 4
    dqc_rule_count: 8
    completed_at: "2026-03-22T11:30:00"
---
```
