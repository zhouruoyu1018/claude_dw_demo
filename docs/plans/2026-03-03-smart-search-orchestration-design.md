# Smart Search Orchestration Design

**Date**: 2026-03-03
**Author**: Claude + User
**Status**: Approved
**Scope**: search-hive-metadata skill enhancement

## Problem

Business users describe data needs using colloquial terms (e.g., "首逾金额", "下款", "过件") that don't match the standardized terminology in Hive table/column comments (e.g., "M1逾期金额", "放款入账", "审批通过"). The current `search_by_comment` uses single-term `LIKE '%keyword%'` matching, resulting in frequent zero-hit searches.

## Solution: Skill-Level Smart Search Orchestration

Leverage Claude's semantic understanding as the "intelligent middle layer" — Claude handles term decomposition, synonym expansion, and result ranking; MCP tools remain unchanged as simple wide-recall backends.

### Design Principles

1. **Zero MCP code changes** — all improvements at the skill prompt level
2. **Claude as the ranking engine** — no scoring algorithm in MCP; Claude's language understanding outperforms `LIKE` + weight formulas for semantic matching
3. **Wide recall, strict selection** — cast a wide net with multiple searches, then let Claude filter
4. **Synonym dictionary as a bootstrap** — `synonym.yaml` provides domain-specific mappings; Claude supplements with its own knowledge

## Architecture

```
Business Term (e.g., "贷款首逾金额")
    │
    ▼
┌─────────────────────────────────────────────┐
│  Step S1: Load synonym.yaml                 │
│  Step S2: Decompose → ["贷款","首逾","金额"] │
│  Step S3: Expand synonyms per unit          │
│    "首逾" → ["M1逾期","首次逾期",...]       │
│  Step S4: Parallel MCP calls (max 4)        │
│    search_by_comment("M1逾期")              │
│    search_by_comment("首次逾期")            │
│    search_by_comment("首逾")                │
│    search_table("overdue")                  │
│  Step S5: Cross-rank results (Claude)       │
│  Step S6: Confirm or ask user               │
└─────────────────────────────────────────────┘
    │
    ▼
Existing disambiguation strategy (unchanged)
```

## Deliverables

### 1. synonym.yaml (NEW)

**Location**: `search-hive-metadata/references/synonym.yaml`

Structure:
```yaml
version: "1.0"
domain: "贷款业务"
synonyms:
  <业务俗称>: [<标准术语1>, <标准术语2>, ...]
```

Content organized by loan lifecycle:
- 进件/申请 (~6 entries)
- 授信/审批 (~4 entries)
- 放款/支用 (~5 entries)
- 贷后/逾期/催收 (~8 entries)
- 通用金融术语 (~6 entries)
- 通用度量词 (~4 entries)
- 维度/属性 (~6 entries)

Total: ~40 synonym groups, ~80 lines.

### 2. SKILL.md — New "Smart Search Orchestration" Section

**Insert position**: After "核心原则: 复用优先" section, before "交互式消歧".

New section defines the 6-step orchestration process (S1-S6):

| Step | Action | Key Rules |
|------|--------|-----------|
| S1 | Load synonym.yaml | Read once per session; skip if unavailable |
| S2 | Decompose compound terms | Claude splits by semantic units, not characters |
| S3 | Expand synonyms | Dictionary first, Claude domain knowledge second |
| S4 | Parallel multi-route recall | Max 4 MCP calls; prioritize high-discrimination terms |
| S5 | Cross-rank results | Multi-hit tables rank higher; dm/da preferred |
| S6 | Confirm strategy | Unique → use; ambiguous → ask user; zero → escalate |

Key design decisions:
- **Discrimination priority**: "M1逾期" (high discrimination) searched before "金额" (low discrimination, matches hundreds of fields)
- **Max 4 calls**: Prevents over-expansion; select the 2-3 most discriminative terms
- **Fallback**: If synonym.yaml unavailable, Claude uses own domain knowledge only — still better than current single-LIKE

### 3. search-examples.md Updates

- Replace "搜索无结果时的处理" with reference to smart orchestration
- Add Scenario 5: end-to-end smart search example
- Update "常见业务术语映射" table to reference synonym.yaml

## Files Changed

| File | Operation | ~Lines |
|------|-----------|--------|
| `references/synonym.yaml` | **CREATE** | ~80 |
| `SKILL.md` | **EDIT** | +~60 |
| `references/search-examples.md` | **EDIT** | +~40 |

## Files NOT Changed

- `mcp_server.py` — No backend changes
- `dw-requirement-triage/SKILL.md` — Caller automatically benefits
- `disambiguation-strategy.md` — Downstream process unchanged
- `scoring-algorithm.md` — Scoring logic unchanged

## Success Criteria

1. "首逾金额" → should find overdue-related tables (currently returns 0)
2. "下款" → should find loan disbursement tables (currently returns 0)
3. "过件率" → should find approval rate indicators (currently returns 0)
4. No regression for terms that already match directly (e.g., "放款金额")

## Future Enhancements (Not in Scope)

- **Feedback loop**: Auto-append user-confirmed mappings to synonym.yaml
- **MCP search_by_comment_v2**: Multi-term OR query + match scoring
- **MySQL trigram index**: For large-scale fuzzy matching performance
