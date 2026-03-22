# Phase 执行详解

> 本文件是 `dw-dev-workflow` 各 Phase 的详细执行规则和说明性内容。
> **强制动作**（skill 调用、文件输出、记忆写入、GATE 门控）已提升到 SKILL.md 的 `☑️ 必做清单` 中，本文件仅保留执行细节说明。
> Phase 4（ETL 开发）因包含关键模式分支逻辑，保留在 SKILL.md 中。

---

## Phase 1: 需求拆解 (dw-requirement-triage)

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

**Checkpoint 写入**（GATE 通过后）: 将维度、粒度、分层、引擎、指标数量、复杂度评分写入 `phase_checkpoints.phase1`，更新 `phase_completed: 1`。详见 [req-file-schema.md](req-file-schema.md)。

> 持久化和用户确认见 SKILL.md Phase 1 必做清单 + GATE。

---

## Phase 2: 复用检索 (search-hive-metadata)

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

**Checkpoint 写入**（GATE 通过后）: 将复用决策、匹配指标、粒度匹配、候选表、词根缓存写入 `phase_checkpoints.phase2`，更新 `phase_completed: 2`。词根缓存 (`word_roots_cached`) 供 Phase 3 直接使用，避免重复查询。

> 用户确认见 SKILL.md Phase 2 GATE。

---

## Phase 3: 模型设计 (generate-standard-ddl)

**执行内容**:
1. 确定分层和表名（命名规范：`dmm_sac_{主题}_{粒度}`）
2. 建模决策：
   - **CASE A (扩列)**: 现有表粒度匹配 → `ALTER TABLE ADD COLUMN`
   - **CASE B (新建)**: 无匹配表 → `CREATE TABLE`
   - **CASE C (冲突)**: 粒度匹配但业务跨度大 → 询问用户
3. 字段命名：查词根表，组装标准字段名
4. 生成 DDL（含 COMMENT、TBLPROPERTIES、逻辑主键）

**输出**: 完整的 CREATE TABLE 或 ALTER TABLE 语句

**Checkpoint 写入**（GATE 通过后）: 将建模决策(A/B/C)、DDL 类型(create/alter)、DDL 路径、字段数量、分区字段、词根校验结果写入 `phase_checkpoints.phase3`，更新 `phase_completed: 3`。`ddl_type` 直接决定 Phase 4 的 ETL 模式。

> 触发条件、skill 调用、记忆写入、用户确认见 SKILL.md Phase 3 必做清单 + GATE。

---

## Phase 4.5: SQL 审查 (review-sql) — 可选

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

> skill 调用、修复循环、用户确认见 SKILL.md Phase 4.5 必做清单。

**Checkpoint 写入**（审查通过后，仅 `--review` 模式）: 将审查结果（fatal/error/warn 计数）写入 `phase_checkpoints.phase4.review_result`。

**默认行为**: 跳过（不加 `--review` 时不执行此阶段）

---

## Phase 5: 测试套件 (generate-qa-suite)

**触发条件**: Phase 4（或 Phase 4.5）完成

**执行内容**:
1. 解析 ETL 上下文（源表、目标表、主键、字段类型）
2. 生成冒烟测试（行数、主键唯一性、NULL 检查、样本抽查）
3. 生成 DQC 规则（完整性、唯一性、有效性、一致性、波动率）
4. Doris 专项：生成 EXPLAIN 分析请求

**输出**: 完整的 QA Suite（冒烟测试 + DQC 规则 + 性能分析）

**Checkpoint 写入**（GATE 通过后）: 将 QA 文件路径、冒烟测试数量、DQC 规则数量写入 `phase_checkpoints.phase5`，更新 `phase_completed: 5`，同时 `status` 改为 `done`。

> skill 调用、状态更新、用户确认见 SKILL.md Phase 5 必做清单 + GATE。
