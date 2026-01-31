---
name: generate-standard-ddl
description: 模型设计师。根据对齐后的需求字段列表，生成符合数仓规范的 CREATE TABLE / ALTER TABLE 语句。使用场景：(1) dw-requirement-triage 输出需求后需要建表 (2) 已有表需要新增字段 (3) 需要标准化的 DDL 语句（含分区、存储格式、逻辑主键、COMMENT）
---

# 模型设计 (Generate Standard DDL)

根据对齐后的需求字段列表，自动生成符合数仓规范的 DDL 语句。

## 定位

**设计师角色** — 不负责数据加工逻辑，只负责"表长什么样"。

## 输入

| 来源 | 内容 |
|------|------|
| `dw-requirement-triage` 输出 | 对齐后的需求字段列表（指标、维度、时间粒度、建议分层） |
| 数仓规范 | 本 Skill 内置（命名、分层、存储格式等） |
| 用户补充 | 可选：业务主题、特殊要求 |

## 输出

标准的 `CREATE TABLE` 或 `ALTER TABLE ADD COLUMN` 语句，包含：
- 分区定义 (PARTITIONED BY)
- 存储格式 (STORED AS)
- 表属性 (TBLPROPERTIES)，**Hive 表必须包含逻辑主键声明**
- 每个字段和表的 COMMENT

---

## 核心工作流

```
需求字段列表
    ↓
┌──────────────────────────────┐
│ Step 1: 确定分层与表名       │
│ dm → dmm_sac_xxx             │
│ da → da_sac_xxx              │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 2: 建模决策             │
│ 搜索现有表 → 扩列 or 新建   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 3: 字段命名             │
│ 查词根表 → 组装标准字段名   │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 4: 字段排序与类型       │
│ 按规范排列，选择数据类型     │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│ Step 5: 生成 DDL             │
│ 拼装完整 CREATE TABLE 语句   │
└──────────────────────────────┘
    ↓
输出标准 DDL
```

---

## Step 1: 确定分层与表名

### 分层选择

| 分层 | 适用场景 | 表名前缀 |
|------|---------|----------|
| **dm** (数据集市) | 业务指标宽表，可被多个报表/接口复用 | `dmm_sac_` |
| **da** (数据应用) | 面向特定报表/接口的最终数据 | `da_sac_` |

### 表命名规则

```
{分层前缀}_{业务主题}_{数据粒度}[_{补充说明}]
```

**示例：**

| 需求 | 分层 | 表名 |
|------|------|------|
| 按日+产品统计放款指标 | dm | `dmm_sac_loan_prod_daily` |
| 按日+渠道统计逾期指标 | dm | `dmm_sac_overdue_chn_daily` |
| 逾期分析看板数据 | da | `da_sac_overdue_analysis` |
| 催收明细导出 | da | `da_sac_collect_export` |

**业务主题关键词：**

| 业务域 | 主题词 |
|--------|--------|
| 进件 | `apply` |
| 授信 | `credit` |
| 签约 | `sign` |
| 放款 | `loan` / `disburse` |
| 还款 | `repay` |
| 逾期 | `overdue` |
| 催收 | `collect` |
| 核销 | `writeoff` |

**粒度关键词：**

| 粒度 | 后缀 |
|------|------|
| 日 | `_daily` |
| 周 | `_weekly` |
| 月 | `_monthly` |
| 明细 | `_dtl` |

---

## Step 2: 建模决策 (Schema Evolution)

在生成 DDL 前，**必须执行以下检查**，决定是新建表还是扩列：

### 2.1 提取维度

从需求中提取 Group By Keys（维度列），例如：
- 需求："按日+产品+渠道统计放款金额" → 维度 = `(dt, product_code, channel_code)`

### 2.2 候选搜索

调用 `search_hive_metadata` 的 `search_table` 或 `search_by_comment` 搜索同主题表。

### 2.3 决策逻辑

| 情况 | 条件 | 动作 |
|------|------|------|
| **CASE A: 扩列** | 找到现有表，维度相同，业务主题相近 | 生成 `ALTER TABLE ADD COLUMN` |
| **CASE B: 新建** | 未找到表，或粒度不匹配 | 生成 `CREATE TABLE` |
| **CASE C: 冲突** | 粒度相同但业务跨度大 | 询问用户 |

**判断维度匹配：**
- 比较分区键（PARTITIONED BY）
- 比较主键/维度列（非指标列：STRING/INT 类型，列名含 `_id`/`_code`/`_name`）
- 候选表维度 = 需求维度 → 匹配
- 候选表维度 ⊃ 需求维度 → 候选更细，不适合直接加列
- 候选表维度 ⊂ 需求维度 → 候选更粗，不匹配

**判断主题相近：**
- 同属贷款销售域（apply/credit/sign/loan）→ 相近
- 同属贷后管理域（repay/overdue/collect/writeoff）→ 相近
- 跨域（如 loan vs overdue）→ 不相近，走 CASE C

**输出要求：** 必须在回复中告知用户决策理由。例如：
> "检测到现有表 `dmm_sac_loan_prod_daily` 粒度与新指标一致（日+产品），建议在该表中新增字段，而不是新建表。"

---

## Step 3: 字段命名

### 3.1 词根查询

所有字段命名必须基于词根表。通过 `search-hive-metadata` MCP Server 的 `search_word_root` 工具查询：

```
调用: search_word_root(keyword="金额")
返回: english_abbr=amt, chinese_name=金额, english_name=amount, tag=CATEGORY_WORD
→ 字段命名使用 english_abbr 列的值: amt

调用: search_word_root(keyword="是否")
返回: english_abbr=is, chinese_name=是否, english_name=is, tag=BOOL
→ 字段命名使用: is_
```

可按标签筛选浏览：`search_word_root(keyword="", tag="CONVERGE")` 查看所有聚合类词根。

可用标签：`BIZ_ENTITY`(业务实体)、`CATEGORY_WORD`(分类词)、`BOOL`(布尔)、`CONVERGE`(聚合)、`TIME`(时间)

如果词根表中没有匹配项，使用通用英文缩写并在 COMMENT 中标注"（新词根，待入库）"。

### 3.2 字段命名组装顺序

字段名**严格按照词根表的 tag 类型**决定位置，组装顺序如下：

```
{BOOL}_{TIME}_{CONVERGE}_{BIZ_ENTITY}_{CATEGORY_WORD}
```

**tag 与位置对应关系：**

| 顺序 | tag | 说明 | 示例词根 |
|------|-----|------|----------|
| 1 | `BOOL` | 布尔前缀 | `is_`, `has_` |
| 2 | `TIME` | 时间范围 | `td_`(当日), `ytd_`(年累计), `his_`(历史) |
| 3 | `CONVERGE` | 聚合方式 | `sum_`, `cnt_`, `avg_`, `max_`, `min_`, `rat_` |
| 4 | `BIZ_ENTITY` | 业务实体 | `loan`, `repay`, `overdue`, `credit` |
| 5 | `CATEGORY_WORD` | 分类词 | `amt`, `cnt`, `days`, `mtd`, `m1`, `normal` |

**重要：必须查词根表确认 tag，不能凭语义猜测位置。**

**完整示例：**

| 业务含义 | 字段名 | 词根拆解 (tag) |
|---------|--------|----------------|
| 是否首次逾期 | `is_first_overdue` | is(BOOL) + first(BIZ_ENTITY) + overdue(BIZ_ENTITY) |
| 当日放款金额 | `td_sum_loan_amt` | td(TIME) + sum(CONVERGE) + loan(BIZ_ENTITY) + amt(CATEGORY_WORD) |
| 月累计放款金额 | `sum_loan_amt_mtd` | sum(CONVERGE) + loan(BIZ_ENTITY) + amt(CATEGORY_WORD) + mtd(CATEGORY_WORD) |
| 历史最大逾期天数 | `his_max_overdue_days` | his(TIME) + max(CONVERGE) + overdue(BIZ_ENTITY) + days(CATEGORY_WORD) |
| 正常还款笔数 | `cnt_repay_normal` | cnt(CONVERGE) + repay(BIZ_ENTITY) + normal(CATEGORY_WORD) |
| M1逾期率 | `rat_overdue_m1` | rat(CONVERGE) + overdue(BIZ_ENTITY) + m1(CATEGORY_WORD) |

**同类 tag 内部排序：** 当多个词根 tag 相同时，按"被修饰词在前，修饰词在后"排列：
- `loan_amt_mtd`：loan_amt(放款金额) 是核心，mtd(月累计) 修饰它

### 3.3 维度字段命名

维度字段（非指标）遵循简洁命名：

| 类型 | 规则 | 示例 |
|------|------|------|
| 主键/ID | `{实体}_id` | `loan_id`, `cust_id` |
| 编码 | `{实体}_code` | `product_code`, `channel_code` |
| 名称 | `{实体}_name` | `product_name`, `channel_name` |
| 日期 | `{实体}_date` | `loan_date`, `apply_date` |
| 状态 | `{实体}_status` | `loan_status`, `overdue_status` |

---

## Step 4: 字段排序与数据类型

### 4.1 字段排序规范

DDL 中字段按以下顺序排列：

```
1. 维度字段（主键/ID → 编码 → 名称 → 日期 → 状态）
2. 布尔字段（is_ / has_ 开头）
3. 时间类指标（td_ → cur_mon_ → his_）
4. 聚合指标（sum_ → cnt_ → avg_ → max_ → min_ → rat_）
5. 其他业务字段
```

### 4.2 数据类型选择

| 业务类型 | Hive 类型 | 说明 |
|---------|-----------|------|
| 主键/ID | `BIGINT` | 数字型标识 |
| 编码 | `STRING` | 变长字符 |
| 名称 | `STRING` | 变长字符 |
| 金额 | `DECIMAL(18,2)` | 精确到分 |
| 件数/笔数 | `BIGINT` | 计数类 |
| 比率/占比 | `DECIMAL(10,4)` | 精确到万分位 |
| 天数 | `INT` | 整数天 |
| 布尔 | `TINYINT` | 0/1 |
| 日期 | `STRING` | 格式 YYYY-MM-DD，与分区格式统一 |
| 时间戳 | `TIMESTAMP` | 精确到秒 |
| 大文本/JSON | `STRING` | Hive 不支持原生 JSON 类型 |

---

## Step 5: 生成 DDL

### 5.1 CREATE TABLE 模板 (Hive)

```sql
-- ============================================================
-- 表名:    {schema}.{table_name}
-- 功能:    {表功能描述}
-- 作者:    {author}
-- 创建日期: {YYYY-MM-DD}
-- 修改记录:
--   {YYYY-MM-DD} {author} 初始创建
-- ============================================================

DROP TABLE IF EXISTS {schema}.{table_name};

CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    -- ===== 维度字段 =====
    {dim_col_1}    {TYPE}    COMMENT '{中文注释}',
    {dim_col_2}    {TYPE}    COMMENT '{中文注释}',

    -- ===== 布尔字段 =====
    {bool_col}     TINYINT   COMMENT '{中文注释}，0-否 1-是',

    -- ===== 指标字段 =====
    {metric_col_1} {TYPE}    COMMENT '{中文注释}',
    {metric_col_2} {TYPE}    COMMENT '{中文注释}'
)
COMMENT '{表中文注释}[粒度:{维度字段1},{维度字段2}]'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'logical_primary_key' = '{col1},{col2},...',
    'business_owner' = '{业务负责人}',
    'data_layer' = '{dm|da}'
);
```

### 5.2 ALTER TABLE 模板

当走 CASE A（扩列）时：

```sql
-- ============================================================
-- 表名:    {schema}.{table_name}
-- 变更:    新增 {N} 个字段 — {变更原因}
-- 作者:    {author}
-- 变更日期: {YYYY-MM-DD}
-- ============================================================

ALTER TABLE {schema}.{table_name} ADD COLUMNS (
    {new_col_1}    {TYPE}    COMMENT '{中文注释}',
    {new_col_2}    {TYPE}    COMMENT '{中文注释}'
) CASCADE;
```

**重要提示：**
- **分区表必须加 `CASCADE`**：确保新增字段应用到所有已存在的分区，否则旧分区无法查到新字段
- 非分区表可省略 `CASCADE`，但加上也无副作用

### 5.3 TBLPROPERTIES 必填项

Hive 表的 TBLPROPERTIES **必须**包含以下属性：

| 属性 | 说明 | 示例 |
|------|------|------|
| `logical_primary_key` | **必填** — 逻辑主键，逗号分隔 | `'loan_id,dt'` |
| `orc.compress` | 压缩算法 | `'SNAPPY'` |
| `business_owner` | 业务归属人/团队 | `'数据组'` |
| `data_layer` | 所属分层 | `'dm'` 或 `'da'` |

### 5.4 分区策略

| 场景 | 分区方式 |
|------|---------|
| 日粒度聚合 | `PARTITIONED BY (dt STRING)` |
| 多维分区（少量枚举） | `PARTITIONED BY (dt STRING, product_code STRING)` |
| 全量快照 | `PARTITIONED BY (dt STRING)` |

---

## COMMENT 规范

每个字段和表都必须有 COMMENT，要求：

1. **表 COMMENT**: 说明业务含义、更新频率，**末尾必须附加粒度声明**
   - 格式: `'{业务描述}，{更新频率}[粒度:{维度1},{维度2},...]'`
   - 示例: `'贷款产品日维度指标宽表，T+1更新[粒度:product_code,dt]'`
   - 粒度字段使用物理字段名，逗号分隔，包含分区字段

2. **维度字段 COMMENT**: 说明业务含义
   - 示例: `'产品编码'`, `'渠道名称'`

3. **指标字段 COMMENT**: 说明计算口径
   - 示例: `'当日放款总金额，单位：元'`, `'M1逾期率=M1逾期本金/应还本金'`

4. **布尔字段 COMMENT**: 必须注明 0/1 含义
   - 示例: `'是否首次逾期，0-否 1-是'`

5. **枚举字段 COMMENT**: 列出枚举值
   - 示例: `'还款状态，1-正常 2-逾期 3-核销'`

---

## 完整示例

**输入需求：** 按日+产品维度统计放款金额、放款笔数、平均授信额度

**Step 1:** 分层 = dm，表名 = `dmm_sac_loan_prod_daily`

**Step 2:** 搜索候选表 → 未找到同粒度同主题表 → CASE B 新建

**Step 3:** 查词根表 → loan_amt(放款金额), loan(放款), credit_amt(授信额度)

**Step 4:** 排序 + 类型选择

**Step 5:** 生成 DDL

```sql
-- ============================================================
-- 表名:    dm.dmm_sac_loan_prod_daily
-- 功能:    贷款产品日维度指标宽表
-- 作者:    auto-generated
-- 创建日期: 2026-01-27
-- 修改记录:
--   2026-01-27 auto-generated 初始创建
-- ============================================================

DROP TABLE IF EXISTS dm.dmm_sac_loan_prod_daily;

CREATE TABLE IF NOT EXISTS dm.dmm_sac_loan_prod_daily (
    -- ===== 维度字段 =====
    product_code          STRING          COMMENT '产品编码',
    product_name          STRING          COMMENT '产品名称',

    -- ===== 指标字段 =====
    td_sum_loan_amt       DECIMAL(18,2)   COMMENT '当日放款总金额，单位：元',
    td_cnt_loan           BIGINT          COMMENT '当日放款笔数',
    td_avg_credit_amt     DECIMAL(18,2)   COMMENT '当日平均授信额度，单位：元'
)
COMMENT '贷款产品日维度指标宽表，T+1更新[粒度:product_code,dt]'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'logical_primary_key' = 'product_code,dt',
    'business_owner' = '数据组',
    'data_layer' = 'dm'
);
```

---

## 与其他 Skill 的协作

```
需求文档
    ↓
dw-requirement-triage        ← 需求拆解，输出字段列表
    ↓
search-hive-metadata         ← 搜索现有表/指标，复用判断
    ↓
generate-standard-ddl        ← 本 Skill：生成标准 DDL
    ↓
DML/ETL 开发                 ← 后续数据加工
```

### 前置依赖

| Skill | 提供内容 |
|-------|---------|
| `dw-requirement-triage` | 对齐后的需求字段列表（指标名、维度、时间粒度、建议分层） |
| `search-hive-metadata` | 现有表搜索结果（用于建模决策 Step 2）、词根查询 |

### 触发方式

本 Skill 在以下场景自动触发：
1. `dw-requirement-triage` 完成输出后，用户确认需要建表
2. 用户直接请求 "帮我建一张 dm/da 表"
3. 用户提出新指标需求且经建模决策判断需要新建表或扩列

## References

- [references/naming-convention.md](references/naming-convention.md) - 完整命名规范与词根查询指南
- [references/ddl-templates.md](references/ddl-templates.md) - Hive / Impala / Doris DDL 模板
