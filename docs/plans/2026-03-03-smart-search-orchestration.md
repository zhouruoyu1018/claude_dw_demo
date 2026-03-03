# Smart Search Orchestration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve search-hive-metadata skill hit rate by adding synonym dictionary + intelligent search orchestration, with zero MCP code changes.

**Architecture:** Add a `synonym.yaml` file mapping business colloquialisms to standard metadata terms. Insert a new "Smart Search Orchestration" section in `SKILL.md` that instructs Claude to decompose terms, expand synonyms, run parallel MCP searches, and cross-rank results. Update `search-examples.md` with a new scenario and updated best practices.

**Tech Stack:** YAML (dictionary), Markdown (skill prompts)

**Design doc:** `docs/plans/2026-03-03-smart-search-orchestration-design.md`

---

### Task 1: Create synonym.yaml

**Files:**
- Create: `.claude/skills/search-hive-metadata/references/synonym.yaml`

**Step 1: Create the synonym dictionary file**

Write the complete file with the following content structure:

```yaml
# ============================================================
# 贷款业务同义词词典 (Loan Business Synonym Dictionary)
# ============================================================
# 用途: search-hive-metadata 智能搜索编排的查询改写依据
# 格式: 业务俗称 → [标准术语候选列表]
#       标准术语 = 元数据 table_comment / column_comment 中常见的措辞
# 维护: 遇到新的搜索失败 case 时手动追加条目
# ============================================================

version: "1.0"
domain: "贷款业务"

synonyms:

  # ── 进件 / 申请 ──────────────────────────────────
  进件:       [申请, 进件, 授信申请, apply]
  报件:       [申请, 进件, 报件, apply]
  申请件:     [申请, 进件, 申请件]
  件量:       [进件量, 申请量, 申请笔数, 进件笔数]
  申请金额:   [申请金额, 申请额度, 进件金额, apply_amount]
  申请通过:   [申请通过, 进件通过, 初审通过]

  # ── 授信 / 审批 ──────────────────────────────────
  批核:       [审批, 授信, 批核, 审批通过, approve, credit]
  过件:       [审批通过, 批核通过, 授信成功, 授信通过]
  拒件:       [审批拒绝, 拒绝, 拒件, 授信拒绝, reject]
  过件率:     [审批通过率, 批核率, 授信通过率, 通过率, approval_rate]
  额度:       [授信额度, 审批额度, 批核额度, 可用额度, credit_limit]

  # ── 签约 / 合同 ──────────────────────────────────
  签约:       [签约, 合同签署, 签订, contract, sign]
  合同:       [合同, 借据, 借款合同, contract]
  合同金额:   [合同金额, 签约金额, 借据金额, contract_amount]

  # ── 放款 / 支用 ──────────────────────────────────
  下款:       [放款, 下款, 出款, 发放, loan, disburse]
  放款成功:   [放款入账, 放款成功, 到账, 发放成功]
  提款:       [支用, 提款, 提现, 动支, withdraw, drawdown]
  放款金额:   [放款金额, 放款本金, 发放金额, 支用金额, loan_amount]
  放款笔数:   [放款笔数, 放款件数, 发放笔数, loan_count]
  首放:       [首次放款, 首笔放款, 新放, first_loan]

  # ── 还款 ────────────────────────────────────────
  还款:       [还款, 回款, 还本, 还息, repay, repayment]
  正常还款:   [正常还款, 按期还款, 正常回款]
  提前还款:   [提前还款, 提前结清, 提前清偿, early_repay]
  结清:       [结清, 清偿, 还清, 全部还款, settle]
  应还:       [应还, 应还款, 应收, 到期应还]
  实还:       [实还, 实际还款, 已还, 已还款]

  # ── 逾期 / 贷后 ──────────────────────────────────
  首逾:       [M1逾期, 首次逾期, 首期逾期, 首逾, first_overdue]
  逾期:       [逾期, 欠款, 拖欠, 违约, overdue, delinquent]
  不良:       [不良, 不良贷款, 不良资产, non_performing, npl]
  迁徙:       [迁徙, 迁徙率, 逾期迁徙, 状态迁移, migration]
  DPD:        [逾期天数, DPD, 拖欠天数, days_past_due]
  M1:         [M1, M1逾期, 逾期1期, 逾期30天]
  M2:         [M2, M2逾期, 逾期2期, 逾期60天]
  M3:         [M3, M3逾期, 逾期3期, 逾期90天]
  M3+:        [M3+, M3+逾期, 逾期90天以上, 严重逾期]

  # ── 催收 / 核销 ──────────────────────────────────
  催收:       [催收, 催款, 催记, 电催, 委外催收, collection]
  核销:       [核销, 坏账核销, 销账, 资产核销, write_off]
  回收:       [回收, 催回, 收回, 已回收, recovery]
  入催:       [入催, 进入催收, 转催收]

  # ── 通用金融术语 ─────────────────────────────────
  本金:       [本金, 贷款本金, 本金余额, 剩余本金, principal]
  利息:       [利息, 应收利息, 利息收入, 贷款利息, interest]
  罚息:       [罚息, 逾期罚息, 违约金, 逾期利息, penalty]
  手续费:     [手续费, 服务费, 费用, 管理费, fee]
  利率:       [利率, 年化利率, 贷款利率, 年利率, interest_rate]
  余额:       [余额, 贷款余额, 在贷余额, 未还余额, balance, outstanding]

  # ── 通用度量词 ──────────────────────────────────
  金额:       [金额, 余额, 总额, 合计, amount, amt, balance]
  笔数:       [笔数, 件数, 单数, 数量, count, cnt]
  比率:       [比率, 比例, 占比, 率, rate, ratio, pct]
  天数:       [天数, 日数, 期限, 自然日, days]

  # ── 维度 / 属性 ──────────────────────────────────
  渠道:       [渠道, 来源, 来源渠道, 获客渠道, channel, source]
  产品:       [产品, 产品类型, 贷款品种, 产品名称, product]
  客户:       [客户, 用户, 借款人, 借户, 贷款人, customer, borrower, user]
  机构:       [机构, 组织, 部门, 分支, 网点, 营业部, org, dept, branch]
  期数:       [期数, 期次, 期限, 还款期, 贷款期限, term, period, tenor]
  日期:       [日期, 统计日期, 数据日期, 业务日期, stat_date, biz_date]
```

**Step 2: Verify the file is valid YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('.claude/skills/search-hive-metadata/references/synonym.yaml')); print('VALID')"`

Expected: `VALID`

**Step 3: Commit**

```bash
git add .claude/skills/search-hive-metadata/references/synonym.yaml
git commit -m "feat(search): add loan business synonym dictionary

Bootstrap synonym.yaml with ~50 synonym groups covering the full
loan lifecycle (apply/credit/disburse/repay/overdue/collection)
plus common financial terms, measures, and dimensions."
```

---

### Task 2: Add Smart Search Orchestration section to SKILL.md

**Files:**
- Modify: `.claude/skills/search-hive-metadata/SKILL.md:79-81` (insert new section between "字段命名校验闭环" ending and "交互式消歧")

**Step 1: Insert new section**

After line 80 (`---`) and before line 82 (`## 交互式消歧`), insert the following new section:

```markdown
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

1. **词典命中**: 在 synonym.yaml 中查找，取全部标准术语
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
2. **分层偏好**: dm/da 层 > dws > dwd > ods（复用现有分层优先级）
3. **语义相关性**: Claude 判断字段注释与原始业务术语的语义距离

### Step S6: 确认策略（Confirm Strategy）

| 情况 | 处理 |
|------|------|
| 唯一高置信结果 | 直接采用，告知用户匹配路径 |
| Top1 vs Top2 差距小 | 展示 Top3 候选，让用户选择 |
| 零命中 | 告知用户所有尝试过的搜索词，建议提供更多线索或换个说法 |

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

- synonym.yaml 不可用 → 仅用 Claude 领域知识做 S3 扩展
- MCP 工具不可用 → 按现有"MCP 不可用时的降级策略"处理
```

**Step 2: Update the "功能概览" bullet list**

At line 14 of SKILL.md, after the "业务术语搜索" bullet, add a note about smart orchestration:

Replace:
```markdown
- **业务术语搜索**: 根据中文注释查找相关表和字段
```

With:
```markdown
- **业务术语搜索**: 根据中文注释查找相关表和字段（支持智能搜索编排：拆词 + 同义词扩展 + 多路召回）
```

**Step 3: Update the "与 dw-requirement-triage 协作" flow diagram**

Replace lines 112-127 (the collaboration flow) to show smart search as part of the flow:

Replace:
```markdown
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
    确认口径         search_by_comment
        ↓              (搜索表/字段)
    直接复用               ↓
                    从 ODS/DWD 开发
```
```

With:
```markdown
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
```

**Step 4: Add synonym.yaml to References section**

At line 166, add one more reference entry:

After:
```markdown
- [references/search-examples.md](references/search-examples.md) - 搜索示例和最佳实践
```

Insert:
```markdown
- [references/synonym.yaml](references/synonym.yaml) - 贷款业务同义词词典（智能搜索编排用）
```

**Step 5: Verify SKILL.md structure**

Run: `grep -n "^## " .claude/skills/search-hive-metadata/SKILL.md`

Expected output should show the new section in the correct position:
```
## 功能概览
## 核心原则: 复用优先 (Reuse First)
## 前置条件
## MCP 不可用时的降级策略
## MCP Server 部署
## 可用工具
## 智能搜索编排 (Smart Search Orchestration)   ← NEW
## 交互式消歧
## 指标复用场景
## 与 dw-requirement-triage 协作
## 多源消歧策略 (Multi-Source Disambiguation)
## 指标生命周期（闭环）
## References
```

**Step 6: Commit**

```bash
git add .claude/skills/search-hive-metadata/SKILL.md
git commit -m "feat(search): add smart search orchestration to SKILL.md

Insert 6-step orchestration process (S1-S6) for intelligent term
decomposition, synonym expansion, and parallel multi-route recall.
Includes discrimination priority, fast-path for direct hits, and
fallback strategies."
```

---

### Task 3: Update search-examples.md

**Files:**
- Modify: `.claude/skills/search-hive-metadata/references/search-examples.md`

**Step 1: Add Scenario 5 — Smart search orchestration example**

After Scenario 4 (line 84, after the closing ``` of the table detail example), insert:

```markdown

### 场景5: 业务术语无法直接匹配元数据（智能搜索编排）

**用户需求**: "查找首逾金额相关的表"

**直接搜索无结果**:
```
search_by_comment(term="首逾金额") → 0 结果
```

**触发智能搜索编排 (S1-S6)**:

**S2 拆词**:
```
"首逾金额" → ["首逾", "金额"]
```

**S3 同义词扩展** (查 synonym.yaml):
```
"首逾" → ["M1逾期", "首次逾期", "首期逾期", "first_overdue", "首逾"]
"金额" → 区分度低，降低优先级
```

**S4 并行搜索** (选取高区分度候选):
```
search_by_comment("M1逾期")    → 2 张表
search_by_comment("首次逾期")  → 1 张表
search_by_comment("首逾")      → 0 结果（验证原词确实搜不到）
search_table("overdue")        → 1 张表
```

**S5 交叉排序**:
```
1. ph_sac_dwd.dwd_overdue_detail — 命中 "M1逾期" + "首次逾期"（2路命中）
2. ph_sac_dmm.dmm_sac_overdue_daily — 仅命中 "M1逾期"（1路命中，dm层加分）
3. ph_sac_dwd.dwd_loan_detail — 仅命中 search_table("overdue")
```

**S6 确认**: Top1 多路命中优势明显 → 自动选定 `dwd_overdue_detail`，展示匹配路径：
```
找到匹配（通过智能搜索编排）:
  搜索路径: "首逾金额" → 拆词["首逾"] → 同义词["M1逾期"] → 命中
  推荐表: ph_sac_dwd.dwd_overdue_detail
  匹配字段: first_overdue_amt (DECIMAL(18,2)): M1逾期金额
```
```

**Step 2: Replace "4. 常见业务术语映射" section**

Replace lines 164-173 (the hardcoded mapping table):

```markdown
### 4. 常见业务术语映射

贷款业务的完整同义词映射见 [synonym.yaml](synonym.yaml)，覆盖进件、授信、放款、贷后全生命周期。

以下为高频映射示例：

| 业务俗称 | 标准术语（元数据注释中的措辞） |
|---------|--------------------------|
| 进件 | 申请、授信申请 |
| 下款 | 放款、发放 |
| 首逾 | M1逾期、首次逾期 |
| 过件率 | 审批通过率、批核率 |
| 提款 | 支用、动支 |
| 结清 | 清偿、全部还款 |
```

**Step 3: Replace "5. 搜索无结果时的处理" section**

Replace lines 175-180:

```markdown
### 5. 搜索无结果时的处理

当直接搜索无结果时，自动触发**智能搜索编排**流程（见 SKILL.md "智能搜索编排" 章节）：

1. 拆词: 将复合术语拆为最小语义单元
2. 同义词扩展: 查 synonym.yaml + Claude 领域知识
3. 并行多路召回: 高区分度术语优先，最多 4 次搜索
4. 交叉排序: 多路命中优先，dm/da 层优先
5. 若仍零命中: 展示所有已尝试的搜索词，建议用户提供更多线索
```

**Step 4: Verify search-examples.md structure**

Run: `grep -n "^### " .claude/skills/search-hive-metadata/references/search-examples.md`

Expected: Should show 场景1-5 and 最佳实践 1-5.

**Step 5: Commit**

```bash
git add .claude/skills/search-hive-metadata/references/search-examples.md
git commit -m "feat(search): add smart search scenario and update best practices

Add Scenario 5 demonstrating end-to-end smart search orchestration.
Update synonym mapping to reference synonym.yaml. Replace manual
retry guidance with automatic orchestration flow reference."
```

---

### Task 4: Verification

**Files:**
- Read: All 3 modified/created files for final consistency check

**Step 1: Verify synonym.yaml is parseable**

Run: `python3 -c "import yaml; d=yaml.safe_load(open('.claude/skills/search-hive-metadata/references/synonym.yaml')); print(f'Groups: {len(d[\"synonyms\"])}'); print('Sample:', list(d['synonyms'].items())[:3])"`

Expected: `Groups: ~50`, sample entries printed.

**Step 2: Verify SKILL.md section ordering**

Run: `grep -n "^## " .claude/skills/search-hive-metadata/SKILL.md`

Expected: "智能搜索编排" appears between "可用工具" and "交互式消歧".

**Step 3: Verify cross-references**

Run: `grep -c "synonym.yaml" .claude/skills/search-hive-metadata/SKILL.md .claude/skills/search-hive-metadata/references/search-examples.md`

Expected: At least 1 reference in each file.

**Step 4: Verify no broken internal links**

Run: `grep -oP '\[.*?\]\((.*?)\)' .claude/skills/search-hive-metadata/SKILL.md | grep -v http | while read link; do ref=$(echo "$link" | grep -oP '\((.*?)\)' | tr -d '()'); [ -f ".claude/skills/search-hive-metadata/$ref" ] || [ -f ".claude/skills/search-hive-metadata/references/$ref" ] || echo "BROKEN: $ref"; done`

Expected: No "BROKEN" lines. The new `references/synonym.yaml` link should resolve.

**Step 5: Final commit (if any fixups needed)**

If any verification fails, fix and commit:
```bash
git add -A .claude/skills/search-hive-metadata/
git commit -m "fix(search): address verification issues in smart search orchestration"
```
