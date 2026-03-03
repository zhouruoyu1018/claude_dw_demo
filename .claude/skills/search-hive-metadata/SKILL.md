---
name: search-hive-metadata
description: Hive 元数据搜索服务，通过 MCP Server 连接 MySQL 元数据库，根据表名或业务术语搜索物理表、字段列表和表注释。使用场景：(1) 根据业务术语查找对应的数仓表 (2) 查看表的字段结构 (3) 为 dw-requirement-triage 提供元数据补全 (4) 数据开发前的表探索
---

# Hive 元数据搜索 (Search Hive Metadata)

通过 MCP Server 连接 MySQL 元数据库，提供 Hive 表的元数据搜索服务。

## 功能概览

- **指标复用检索**: 优先查询指标库，复用已计算好的 DWS/ADS 层指标
- **表名搜索**: 支持精确匹配和模糊搜索
- **业务术语搜索**: 根据中文注释查找相关表和字段（支持智能搜索编排：拆词 + 同义词扩展 + 多路召回）
- **字段查询**: 获取表的完整字段列表
- **交互式消歧**: 搜索结果不唯一时，提示用户选择
- **数据血缘管理**: 注册和查询表级/字段级血缘，支持影响分析和数据溯源 (v2.0+)

## 核心原则: 复用优先 (Reuse First)

收到指标需求时，严格遵守以下步骤：

1. **Check (查)**: 调用 `search_existing_indicators` 搜索指标库
2. **Verify (验)**: 如果找到，向用户展示该字段的计算口径，确认是否一致
3. **Reuse (用)**: 如果用户确认一致，直接 SELECT 该字段，禁止重新编写计算逻辑
4. **Build (造)**: 只有在确认现有表中没有该指标，或口径不符时，才从 ODS/DWD 层开始重新开发

## 前置条件

1. 已部署 MCP Server 并配置 MySQL 连接
2. MySQL 中已存在 `tbl_base_info` 元数据表
3. Claude Code 已配置 MCP Server 连接

## MCP 不可用时的降级策略

当 MCP Server 连接失败或超时时，本 skill 的工具均不可用。调用方 skill 应按以下策略降级：

- **查询类工具**（search_table、search_by_comment、get_table_detail、list_columns、search_word_root、validate_field_name、search_existing_indicators、search_lineage_*）：告知用户 MCP 不可用，请求用户手动提供所需信息，然后继续流程
- **写入类工具**（register_indicator、register_lineage）：将待注册数据以 JSON 格式输出到脚本注释中，标记 `-- [MCP-PENDING]`，待 MCP 恢复后补录

## MCP Server 部署

部署、配置与常见问题已下沉到参考文档：

- [references/deployment-guide.md](references/deployment-guide.md)

主流程仅依赖以下结论：

1. MCP Server 可启动并可访问元数据库
2. `search_table` / `search_by_comment` / `get_table_detail` 等工具可正常调用
3. 不可用时按本文件"降级策略"继续流程

## 可用工具

MCP Server 提供以下工具（详细参数、返回值与示例见 [references/tool-api-reference.md](references/tool-api-reference.md)）：

| 工具 | 分类 | 用途 |
|------|------|------|
| `search_existing_indicators` | 查询 | **优先使用** — 查指标库，检查目标指标是否已被计算 |
| `search_word_root` | 查询 | 搜索词根表，返回候选缩写 + `match_level/score`，用于最小语义单元命名 |
| `validate_field_name` | 查询 | 校验字段名词根存在性、tag 顺序和命名证据覆盖，DDL 输出前必调 |
| `search_table` | 查询 | 按表名模糊搜索 Hive 表 |
| `search_by_comment` | 查询 | 按中文业务术语搜索表和字段 |
| `get_table_detail` | 查询 | 获取表的完整详情（注释、字段、分区、数据量等） |
| `list_columns` | 查询 | 获取表的字段列表（名称、类型、注释） |
| `register_indicator` | 写入 | 将新指标注册到指标库（自动去重，闭环复用流程） |
| `register_lineage` | 写入 | 注册表级/字段级血缘（ETL 完成后自动调用） |
| `search_lineage_upstream` | 查询 | 查询上游依赖（我依赖谁），支持多层递归 |
| `search_lineage_downstream` | 查询 | 查询下游影响（谁依赖我），用于变更影响评估 |

### 字段命名校验闭环

在 `generate-standard-ddl` 场景中，词根命名必须走完整闭环：

1. `search_word_root`：按最小语义单元查询，优先使用 `match_level=exact` 且 `score` 更高的候选
2. 产出命名证据表：语义单元 → 词根缩写 → tag
3. `validate_field_name`：校验词根存在性、tag 顺序、证据覆盖
4. 若校验失败，禁止输出最终 DDL，必须先修正字段名

---

## 智能搜索编排 (Smart Search Orchestration)

当使用业务术语搜索表和字段时（即调用 `search_by_comment` 或 `search_table`），**不要**直接用原始术语做单次搜索。必须先执行以下编排流程，提升召回率：

```
业务术语 → S1 加载词典 → S2 拆词 → S3 同义词扩展 → S4 并行搜索 → S5 交叉排序 → S6 确认
```

### Step S1: 加载同义词词典

读取 [references/synonym.yaml](references/synonym.yaml)。词典将业务俗称映射到元数据注释中常见的标准术语。

- 同一会话内已加载过则跳过重复读取
- 若文件不可用，跳过此步，仅依赖 S3 中 Claude 自身的领域知识

### Step S2: 拆词（Term Decomposition）

将复合业务术语拆分为**最小语义单元**：

| 输入 | 拆词结果 |
|------|---------|
| 贷款首逾金额 | ["贷款", "首逾", "金额"] |
| 渠道放款笔数 | ["渠道", "放款", "笔数"] |
| 日放款金额 | ["日", "放款", "金额"] |
| 过件率 | ["过件", "率"] |

规则：
- 按语义边界拆分，不是按字符
- 如果原词本身就是最小语义单元（如"逾期"），不再拆分
- 如果不确定拆分方式，保留原词作为一个候选

### Step S3: 同义词扩展（Synonym Expansion）

对每个语义单元，按以下优先级扩展候选查询词：

1. **词典命中**: 在 synonym.yaml 中查找，取全部标准术语。**大小写归一**：查找前先对输入做大小写归一（如 `m1` → `M1`、`dpd` → `DPD`），确保不因大小写差异漏召回
2. **Claude 领域知识**: 补充词典中未覆盖的同义词（如行业术语、英文缩写）
3. **原词保留**: 始终保留原始术语作为候选之一

示例：
```
"首逾" →  词典命中: ["M1逾期", "首次逾期", "首期逾期", "first_overdue"]
          + 原词: ["首逾"]
          → 候选集: ["M1逾期", "首次逾期", "首期逾期", "first_overdue", "首逾"]
```

### Step S4: 并行多路召回（Parallel Multi-Route Recall）

从 S3 的候选集中，选取**区分度最高**的 2-3 个词执行搜索。

**区分度判断原则**：
- 高区分度: 业务专有术语（如"M1逾期"、"催收"、"核销"）→ 优先搜索
- 低区分度: 通用度量词（如"金额"、"笔数"、"日期"）→ 降低优先级或不单独搜索

**搜索策略**：
- 对高区分度术语：调用 `search_by_comment(term=术语, search_scope="all")`
- 对可能的英文表名：调用 `search_table(keyword=英文词)`
- 总调用次数上限: **4 次**（避免过度发散）

示例（"贷款首逾金额"）：
```
search_by_comment("M1逾期")          ← 最高区分度
search_by_comment("首次逾期")        ← 高区分度
search_by_comment("首逾")            ← 原词兜底
search_table("overdue")              ← 英文表名
```

### Step S5: 交叉排序（Cross-Ranking）

合并所有搜索结果，去重后按以下规则排序：

1. **多路命中优先**: 被 2+ 个搜索同时命中的表/字段排在最前
2. **分层偏好**: da/ads > dm/dmm > dws > dwd > ods（与多源消歧策略权重一致）
3. **语义相关性**: Claude 判断字段注释与原始业务术语的语义距离

### Step S6: 确认策略（Confirm Strategy）

| 情况 | 处理 |
|------|------|
| 唯一高置信结果 | 直接采用，告知用户匹配路径 |
| Top1 vs Top2 差距小 | 展示 Top3 候选，让用户选择 |
| 零命中 | 告知用户所有尝试过的搜索词，建议提供更多线索 |

**输出格式**（零命中时）：
```
未找到匹配。已尝试以下搜索：
- search_by_comment("M1逾期") → 0 结果
- search_by_comment("首次逾期") → 0 结果
- search_by_comment("首逾") → 0 结果
- search_table("overdue") → 0 结果

建议：请提供更具体的表名关键词或确认业务术语。
```

### 直接命中时的快速路径

如果原始术语直接搜索就有结果（即 `search_by_comment(原词)` 返回非空），则**跳过 S1-S4**，直接进入现有的消歧/选择流程。智能编排仅在直接搜索无结果或结果明显不相关时触发。

### 降级策略

- synonym.yaml 不可用 → 仅用 Claude 领域知识做 S3 扩展，但**最多扩展 2 个同义词**（避免不可控发散），且必须保留"已尝试搜索词列表"输出给用户
- MCP 工具不可用 → 按现有"MCP 不可用时的降级策略"处理

---

## 交互式消歧

当搜索结果不唯一时，触发交互式选择：

```
找到 5 个匹配的表，请选择：

1. ods.ods_order_info - 订单原始信息表
2. dwd.dwd_order_detail - 订单明细宽表
3. dws.dws_order_daily - 订单日汇总表
4. ads.ads_order_analysis - 订单分析结果表
5. dim.dim_order_status - 订单状态维度表

请输入序号或输入更精确的关键词继续搜索:
```

---

## 指标复用场景

**复用流程**: `search_existing_indicators` 查找 → 展示口径让用户确认 → 用户选 A(复用)则直接 SELECT / 选 B(重算)则启动开发流程。

完整场景示例见 [references/search-examples.md](references/search-examples.md)。

---

## 与 dw-requirement-triage 协作

在需求拆解过程中，可以调用此 skill 进行元数据补全：

```
需求文档 → dw-requirement-triage (提取需求)
                    ↓
            识别业务术语/指标
                    ↓
            search_existing_indicators (优先查指标库)
                    ↓
        ┌─────────┴─────────┐
        ↓                   ↓
    找到指标             未找到
        ↓                   ↓
    确认口径         智能搜索编排 (S1-S6)
        ↓             拆词 + 同义词 + 多路召回
    直接复用               ↓
                    从 ODS/DWD 开发
```

---

## 多源消歧策略 (Multi-Source Disambiguation)

当同一字段在多张候选表中出现时，按统一评分策略选择来源表。

最小决策骨架：

1. 先查指标库口径（`search_existing_indicators`）
2. 再做候选表评分（粒度 / 分层 / 覆盖率）
3. Top1 与 Top2 分差 < 10 时触发用户确认
4. 输出候选分数、选中表与选择理由，供下游 ETL 使用

完整策略、评分细节与交互场景见：

- [references/disambiguation-strategy.md](references/disambiguation-strategy.md)
- [references/scoring-algorithm.md](references/scoring-algorithm.md)

与 `generate-etl-sql` 的协作约定：

- 在其 Step 1（源表识别）阶段调用该策略
- 在 ETL 输出头注释保留"数据来源决策"摘要

---

## 指标生命周期（闭环）

两个工具构成闭环：`search_existing_indicators`（查）→ `register_indicator`（入）。

- **需求阶段**: 先查指标库，找到 → 复用，未找到 → 新建开发
- **开发完成**: `generate-etl-sql` 自动识别新指标，询问用户是否注册为公共指标
- **入库判断**: dm 层通用口径 → 建议入库；da 层或特殊条件 → 询问用户

## References

- [references/tool-api-reference.md](references/tool-api-reference.md) - MCP 工具完整参数与示例
- [references/metadata-schema.md](references/metadata-schema.md) - 元数据表结构详解
- [references/search-examples.md](references/search-examples.md) - 搜索示例和最佳实践
- [references/synonym.yaml](references/synonym.yaml) - 贷款业务同义词词典（智能搜索编排用）
- [references/deployment-guide.md](references/deployment-guide.md) - MCP Server 部署与接入指南
- [references/disambiguation-strategy.md](references/disambiguation-strategy.md) - 多源消歧策略详解
- [references/scoring-algorithm.md](references/scoring-algorithm.md) - 候选表综合评分实现
