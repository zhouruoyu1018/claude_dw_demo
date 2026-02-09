---
name: search-hive-metadata
description: Hive 元数据搜索服务，通过 MCP Server 连接 MySQL 元数据库，根据表名或业务术语搜索物理表、字段列表和表注释。使用场景：(1) 根据业务术语查找对应的数仓表 (2) 查看表的字段结构 (3) 为 dw-requirement-triage 提供元数据补全 (4) 数据开发前的表探索
---

# Hive 元数据搜索 (Search Hive Metadata)

通过 MCP Server 连接 MySQL 元数据库，提供 Hive 表的元数据搜索服务。

## 功能概览

- **指标复用检索**: 优先查询指标库，复用已计算好的 DWS/ADS 层指标
- **表名搜索**: 支持精确匹配和模糊搜索
- **业务术语搜索**: 根据中文注释查找相关表和字段
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

## MCP Server 部署

### 1. 安装依赖

```bash
cd scripts/
pip install -r requirements.txt
```

### 2. 配置数据库连接

编辑 `scripts/config.yaml`:

```yaml
mysql:
  host: your-mysql-host
  port: 3306
  user: your-username
  password: your-password
  database: hive_metadata_db
```

### 3. 启动 MCP Server

```bash
python scripts/mcp_server.py
```

### 4. 配置 Claude Code

在 Claude Code 配置文件中添加 MCP Server:

```json
{
  "mcpServers": {
    "hive-metadata": {
      "command": "python",
      "args": ["E:/claude_demo/search-hive-metadata/scripts/mcp_server.py"],
      "env": {
        "MYSQL_HOST": "your-host",
        "MYSQL_USER": "your-user",
        "MYSQL_PASSWORD": "your-password"
      }
    }
  }
}
```

## 可用工具

MCP Server 提供以下工具：

### search_word_root

搜索词根表 (`word_root_dict`)，获取标准缩写用于字段命名。在 `generate-standard-ddl` 生成 DDL 时必须调用此工具。

**参数:**
- `keyword` (string, required): 搜索关键词，支持中文或英文，如"金额"、"amt"、"还款"
- `tag` (string, optional): 词根分类标签筛选，可选值：`BIZ_ENTITY`(业务实体)、`CATEGORY_WORD`(分类词)、`BOOL`(布尔)、`CONVERGE`(聚合)、`TIME`(时间)
- `limit` (integer, optional): 返回结果数量，默认 20

**返回:**
- `english_abbr`: 英文缩写（**用于字段命名**）
- `chinese_name`: 中文名称
- `english_name`: 英文全称
- `alias`: 别名（可为空）
- `tag`: 分类标签

**示例:**
```
搜索 "金额" → english_abbr=amt, chinese_name=金额, english_name=amount, tag=CATEGORY_WORD
搜索 "是否" → english_abbr=is, chinese_name=是否, english_name=is, tag=BOOL
按标签浏览 → search_word_root(keyword="", tag="CONVERGE") 查看所有聚合类词根
```

---

### search_existing_indicators (优先使用)

在设计新报表前，优先查询指标库，检查目标指标是否已经被计算过。

**参数:**
- `metric_name` (string, required): 业务指标名称，如"复购率"、"GMV"、"日销售额"
- `limit` (integer, optional): 返回结果数量，默认 10

**返回:**
- `match_type`: 匹配精确度 (perfect/high/partial)
- `indicator_name`: 指标名称
- `target_column`: 物理字段名
- `source_table`: 所在表
- `logic_desc`: 口径描述

**示例:**
```
搜索 "复购率" 指标是否已存在
```

---

### register_indicator

将新指标注册到指标库。ETL 开发完成后，对用户确认为公共指标的新指标执行入库，闭环"复用优先"流程。注册前自动检查重复，同名指标会跳过。

**参数:**
- `indicators` (array, required): 待注册的指标列表，每条包含：
  - **必填字段:**
  - `indicator_code` (string): 指标编码，如 `IDX_LOAN_001`
  - `indicator_name` (string): 业务指标名称，如 `当日放款金额`
  - `indicator_english_name` (string): 英文名/物理字段名，如 `td_loan_amt`
  - `indicator_category` (string): 指标分类: `原子指标`/`派生指标`/`复合指标`
  - `business_domain` (string): 业务域，如 `贷款`/`风控`/`营销`
  - `data_type` (string, **枚举**): 从元数据获取的物理字段类型。可选值: `TINYINT`/`SMALLINT`/`INT`/`BIGINT`/`FLOAT`/`DOUBLE`/`DECIMAL`/`STRING`/`VARCHAR`/`CHAR`/`DATE`/`TIMESTAMP`/`BOOLEAN`/`ARRAY`/`MAP`/`STRUCT`
  - `standard_type` (string, **枚举**): 标准类型。可选值: `数值类`/`日期类`/`文本类`/`枚举类`/`时间类`
  - `update_frequency` (string, **枚举**): 更新频率。可选值: `实时`/`每小时`/`每日`/`每周`/`每月`/`每季`/`每年`/`手动`
  - `status` (string, **枚举**): 状态。可选值: `启用`/`未启用`/`废弃`，默认 `启用`
  - **可选字段:**
  - `indicator_alias` (string): 指标别名
  - `statistical_caliber` (string): 业务口径描述
  - `calculation_logic` (string): 取值逻辑，推荐格式: `SELECT 字段 FROM 表 WHERE 条件`；也可为计算公式
  - `data_source` (string): 数据来源表
  - `value_domain` (string): 值域说明
  - `sensitive` (string): 敏感级别
- `created_by` (string, optional): 创建人标识，默认 `auto`

**返回:**
- `registered`: 成功注册的指标列表
- `skipped`: 跳过的指标（同名已存在）
- `failed`: 失败的指标（缺少必填字段或枚举校验失败）
- `summary`: 汇总（total / registered / skipped / failed）

**示例:**
```json
register_indicator({
    "indicators": [
        {
            "indicator_code": "IDX_LOAN_001",
            "indicator_name": "当日放款金额",
            "indicator_english_name": "td_sum_loan_amt",
            "indicator_category": "原子指标",
            "business_domain": "贷款",
            "data_type": "DECIMAL",
            "standard_type": "数值类",
            "update_frequency": "每日",
            "status": "启用",
            "statistical_caliber": "当日所有放款订单金额之和，单位：元",
            "calculation_logic": "SELECT SUM(loan_amt) FROM dwd.dwd_loan_dtl WHERE loan_date = '${dt}' AND status = 'SUCCESS'",
            "data_source": "dm.dmm_sac_loan_prod_daily"
        },
        {
            "indicator_code": "IDX_LOAN_002",
            "indicator_name": "当日放款笔数",
            "indicator_english_name": "td_cnt_loan",
            "indicator_category": "原子指标",
            "business_domain": "贷款",
            "data_type": "BIGINT",
            "standard_type": "数值类",
            "update_frequency": "每日",
            "status": "启用",
            "statistical_caliber": "当日放款订单去重计数",
            "data_source": "dm.dmm_sac_loan_prod_daily"
        }
    ],
    "created_by": "zhangsan"
})
```

---

### search_table

按表名搜索 Hive 表。

**参数:**
- `keyword` (string, required): 表名关键词，支持模糊匹配
- `schema_name` (string, optional): 限定数据库名
- `limit` (integer, optional): 返回结果数量，默认 10

**示例:**
```
搜索包含 "order" 的表
```

### search_by_comment

按业务术语（注释）搜索。

**参数:**
- `term` (string, required): 业务术语，如"申请时间"、"放款金额"
- `search_scope` (string, optional): 搜索范围 - "table"(表注释) | "column"(字段注释) | "all"(全部)
- `limit` (integer, optional): 返回结果数量，默认 10

**示例:**
```
搜索包含 "放款金额" 的表或字段
```

### get_table_detail

获取表的详细信息。

**参数:**
- `table_name_full` (string, required): 完整表名，如 "ods.ods_order_info"

**返回:**
- 表注释、字段列表、分区键、数据量、存储格式等

### list_columns

获取表的字段列表。

**参数:**
- `table_name_full` (string, required): 完整表名

**返回:**
- 字段名、字段类型、字段注释的结构化列表

---

## 数据血缘管理

从 v2.0 开始，MCP Server 支持数据血缘的注册与查询，帮助追踪数据流转和评估变更影响。

### register_lineage

注册表级和字段级血缘关系。在 ETL 开发完成后自动调用（由 `generate-etl-sql` 触发），或手动注册历史 ETL 的血缘。

**参数:**
- `target_table` (string, required): 目标表完整名，如 "dm.dmm_sac_loan_prod_daily"
- `source_tables` (array, required): 源表列表，每项包含:
  - `source_table` (string): 源表完整名
  - `join_type` (string): JOIN 类型 (FROM/LEFT JOIN/INNER JOIN/RIGHT JOIN/FULL JOIN/CROSS JOIN)
  - `relation_type` (string, optional): 关系类型，默认"ETL"，可选"VIEW"、"MANUAL"
- `etl_script_path` (string, optional): ETL 脚本路径
- `etl_logic_summary` (string, optional): ETL 逻辑摘要，如"按产品维度聚合当日放款"
- `column_lineage` (array, optional): 字段级血缘列表，每项包含:
  - `target_column` (string): 目标字段名
  - `source_table` (string): 源表完整名
  - `source_column` (string): 源字段名
  - `transform_type` (string): 转换类型 (DIRECT/SUM/COUNT/AVG/MAX/MIN/CASE/CUSTOM)
  - `transform_expr` (string): 转换表达式，如 "SUM(loan_amount)"
- `created_by` (string, optional): 创建人标识，默认"auto"

**返回:**
- 表级血缘注册数量、字段级血缘注册数量、详细列表

**示例:**
```javascript
register_lineage({
  "target_table": "dm.dmm_sac_loan_prod_daily",
  "source_tables": [
    {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
    {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
  ],
  "etl_logic_summary": "按产品维度聚合当日放款",
  "column_lineage": [
    {
      "target_column": "td_sum_loan_amt",
      "source_table": "dwd.dwd_loan_detail",
      "source_column": "loan_amount",
      "transform_type": "SUM",
      "transform_expr": "SUM(loan_amount)"
    }
  ]
})
```

### search_lineage_upstream

查询表的上游依赖（我依赖谁）。用于数据溯源、问题排查、评估源表变更影响。

**参数:**
- `table_name` (string, required): 表完整名，如 "dm.dmm_sac_loan_prod_daily"
- `depth` (integer, optional): 递归深度，1=仅直接依赖，2=包含二级依赖，默认 1
- `include_columns` (boolean, optional): 是否包含字段级血缘，默认 false

**返回:**
- `upstream_tables`: 上游表列表（按深度分组）
- `column_lineage`: 字段级血缘（如果 include_columns=true）
- `total_upstream`: 上游表总数

**示例:**
```javascript
search_lineage_upstream({
  "table_name": "dm.dmm_sac_loan_prod_daily",
  "depth": 2,
  "include_columns": true
})
```

**返回示例:**
```
## 上游血缘: `dm.dmm_sac_loan_prod_daily`

找到 **4** 个上游依赖:

### 第 1 层依赖
| 源表 | JOIN 类型 | 逻辑摘要 |
|------|----------|----------|
| `dwd.dwd_loan_detail` | FROM | 按产品维度聚合当日放款明细 |
| `dim.dim_product` | LEFT JOIN | 关联产品维度获取产品名称 |

### 第 2 层依赖
| 源表 | JOIN 类型 | 逻辑摘要 |
|------|----------|----------|
| `ods.ods_loan_apply` | FROM | 清洗贷款申请数据 |
| `ods.ods_loan_contract` | LEFT JOIN | 关联合同信息补充放款字段 |
```

### search_lineage_downstream

查询表的下游影响（谁依赖我）。用于评估表变更影响范围、通知下游用户、规划数据迁移。

**参数:**
- `table_name` (string, required): 表完整名，如 "dwd.dwd_loan_detail"
- `depth` (integer, optional): 递归深度，1=仅直接影响，2=包含二级影响，默认 1
- `include_columns` (boolean, optional): 是否包含字段级影响，默认 false

**返回:**
- `downstream_tables`: 下游表列表（按深度分组）
- `column_impact`: 字段级影响（如果 include_columns=true）
- `total_downstream`: 下游表总数

**示例:**
```javascript
search_lineage_downstream({
  "table_name": "dwd.dwd_loan_detail",
  "depth": 2
})
```

**返回示例:**
```
## 下游影响: `dwd.dwd_loan_detail`

找到 **5** 个下游表会受影响:

### 第 1 层影响
| 下游表 | JOIN 类型 | 逻辑摘要 |
|-------|----------|----------|
| `dm.dmm_sac_loan_prod_daily` | FROM | 按产品维度聚合当日放款明细 |
| `dm.dmm_sac_loan_chn_daily` | FROM | 按渠道维度聚合当日放款明细 |
| `dws.dws_cust_loan_summary` | FROM | 按客户维度汇总贷款信息 |

### 第 2 层影响
| 下游表 | JOIN 类型 | 逻辑摘要 |
|-------|----------|----------|
| `da.da_loan_report` | FROM | 汇总产品维度指标到报表层 |

**⚠️ 变更提醒**: 修改此表前，请评估对上述下游表的影响，并通知相关负责人。
```

---

## 交互式消歧

当搜索结果不唯一时，触发交互式选择：

**场景示例:**

用户: "查找订单表"

系统返回多个匹配结果时:
```
找到 5 个匹配的表，请选择：

1. ods.ods_order_info - 订单原始信息表
2. dwd.dwd_order_detail - 订单明细宽表
3. dws.dws_order_daily - 订单日汇总表
4. ads.ads_order_analysis - 订单分析结果表
5. dim.dim_order_status - 订单状态维度表

请输入序号或输入更精确的关键词继续搜索:
```

## 指标复用场景示例

**用户需求:** "我要看昨天的复购率"

**步骤 1: 尝试复用**
```
Agent 调用: search_existing_indicators(metric_name="复购率")
```

**返回结果:**
```json
{
  "match_type": "perfect",
  "indicator_name": "复购率",
  "target_column": "repurchase_rate",
  "source_table": "ads.ads_user_retention_1d",
  "logic_desc": "统计周期内发生2次及以上支付行为的用户占比"
}
```

**步骤 2: 交互确认**
```
检测到 ads.ads_user_retention_1d 表中已包含 复购率 (repurchase_rate) 字段。
口径定义为：统计周期内发生2次及以上支付行为的用户占比。

请问是否直接复用此字段？
(A) 是，直接复用
(B) 否，口径不同，需要重新计算
```

**步骤 3: 差异化处理**
- 用户选 A (复用): 直接生成 `SELECT repurchase_rate FROM ads.ads_user_retention_1d WHERE dt='2024-01-15'`
- 用户选 B (重算): 启动原有流程，去 ODS/DWD 层找原始表重新开发

---

## 与 dw-requirement-triage 协作

在需求拆解过程中，可以调用此 skill 进行元数据补全：

### 工作流程

```
需求文档 → dw-requirement-triage (提取需求)
                    ↓
            识别业务术语/指标（如"放款金额"、"复购率"）
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

### 协作示例

**需求原文:**
> 统计每日放款金额和放款笔数

**dw-requirement-triage 提取:**
- 指标: 放款金额, 放款笔数
- 维度: 日期

**search-hive-metadata 补全:**
```
搜索 "放款金额" → 找到字段 dwd.dwd_loan_detail.loan_amount
搜索 "放款笔数" → 找到字段 dwd.dwd_loan_detail.loan_id (COUNT)

建议数据来源: dwd.dwd_loan_detail
相关字段:
- loan_amount (DECIMAL): 放款金额
- loan_id (BIGINT): 放款ID
- loan_date (DATE): 放款日期
```

## 输出格式

### 表搜索结果

```markdown
## 搜索结果: "order"

| 表名 | 数据库 | 注释 | 数据量 | 更新时间 |
|-----|-------|------|-------|---------|
| ods_order_info | ods | 订单原始信息 | 1.2TB | 2024-01-15 |
| dwd_order_detail | dwd | 订单明细宽表 | 800GB | 2024-01-15 |
```

### 表详情

```markdown
## 表详情: dwd.dwd_order_detail

- **表注释**: 订单明细宽表，包含订单、用户、商品关联信息
- **数据库**: dwd
- **分区键**: dt
- **数据量**: 800GB
- **存储格式**: ORC
- **负责人**: zhangsan
- **更新时间**: 2024-01-15

### 字段列表

| 字段名 | 类型 | 注释 |
|-------|-----|------|
| order_id | BIGINT | 订单ID |
| user_id | BIGINT | 用户ID |
| order_amount | DECIMAL(18,2) | 订单金额 |
| order_time | TIMESTAMP | 下单时间 |
| dt | STRING | 分区字段，格式YYYY-MM-DD |
```

## 多源消歧策略 (Multi-Source Disambiguation)

当同一字段（如 `loan_amt`）出现在多张表中时，使用以下策略选择最优数据源。

### 核心原则

**口径一致 > 粒度匹配 > 分层优先 > 覆盖率**

评分权重分配：
- 口径一致性: 40 分（一票否决 + 加成）
- 粒度匹配: 30 分
- 分层优先级: 20 分
- 字段覆盖率: 10 分（降低权重，避免误导）

### 策略一：口径一致性（最高优先级）

**问题**：同一字段名在不同表可能口径不同！

| 表 | 字段 | 口径 |
|----|------|------|
| `dwd_loan_apply` | loan_amt | 申请金额 |
| `dwd_loan_disburse` | loan_amt | 放款金额 |
| `dws_loan_daily` | loan_amt | 当日累计放款金额 |

**执行步骤**：

1. 先调用 `search_existing_indicators` 查询指标库
2. 如果指标库有记录，检查 `source_table` 字段 → **直接采信，跳过其他策略**
3. 如果指标库无记录，继续后续策略

**评分**：
- 指标库命中 + 口径一致 → +40 分，且跳过其他评分
- 指标库命中 + 口径不符 → 0 分，标记"口径冲突"需用户确认
- 指标库未命中 → 进入策略二

### 策略二：粒度匹配（30 分）

根据目标查询的粒度（GROUP BY 维度），选择粒度最接近的表。

**粒度判断来源**（优先级从高到低）：

1. `TBLPROPERTIES` 中的 `logical_primary_key`
2. 表注释中的 `[粒度:字段1,字段2]`
3. 分区键 + 非指标字段推断

**粒度匹配评分**：

| 匹配情况 | 分数 | 说明 |
|---------|------|------|
| 完全匹配 | 30 | 表粒度 = 查询粒度 |
| 表粒度更细 | 15 | 表粒度 ⊃ 查询粒度，需聚合 |
| 表粒度更粗 | 0 | 表粒度 ⊂ 查询粒度，无法使用 |
| 无法判断 | 10 | 缺少元数据，保守给分 |

**示例**：

```
查询粒度: (product_code, dt)

候选表 A: dmm_sac_loan_prod_daily    粒度=(product_code, dt)     → 30分
候选表 B: dwd_loan_detail            粒度=(loan_id, dt)          → 15分 (需聚合)
候选表 C: dws_loan_monthly           粒度=(product_code, month)  → 0分 (更粗)
```

### 策略三：分层优先级（20 分）

上层表数据已清洗、聚合，优先选择。

| 分层 | 前缀 | 分数 | 说明 |
|------|------|------|------|
| 应用层 | `da_`, `ads_` | 20 | 直接取结果 |
| 集市层 | `dm_`, `dmm_` | 18 | 业务宽表，可复用 |
| 汇总层 | `dws_` | 15 | 预聚合数据 |
| 维度层 | `dim_` | 12 | 维度属性补充 |
| 明细层 | `dwd_` | 8 | 数据量大，计算慢 |
| 原始层 | `ods_` | 2 | 除非没得选 |
| 临时层 | `tmp_` | 0 | 不应作为数据源 |

### 策略四：字段覆盖率（10 分）

当查询需要多个字段时，优先选择包含更多目标字段的表，减少 JOIN。

**公式**：

```
覆盖率分数 = (表内包含的目标字段数 / 总目标字段数) × 10
```

**注意**：权重仅 10 分，避免 ODS 表因"包含所有字段"而被误选。

### 综合评分函数

对每个候选表按四维度逐项计分，详见 [references/scoring-algorithm.md](references/scoring-algorithm.md) 获取 Python 实现。

评分流程：口径一致(40分，指标库命中一票否决) → 粒度匹配(30分) → 分层优先(20分) → 字段覆盖率(10分) → 降序排列输出。

### 决策流程图

```
需求字段列表 + 查询粒度
    ↓
┌─────────────────────────────────┐
│ Step 1: 指标库查询              │  ← search_existing_indicators
│ 检查是否有"官方口径"            │
└─────────────────────────────────┘
    ↓ 找到 → 直接使用 source_table，流程结束
    ↓ 未找到
┌─────────────────────────────────┐
│ Step 2: 元数据搜索              │  ← search_table / search_by_comment
│ 获取候选表列表                  │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Step 3: 综合评分                │
│ 粒度(30) + 分层(20) + 覆盖(10)  │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Step 4: 输出排序结果            │
│ 展示 Top 3 及评分明细           │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Step 5: 交互式确认              │
│ 差距 < 10分 → 询问用户          │
│ 差距 >= 10分 → 自动选择最高分    │
└─────────────────────────────────┘
```

### 交互式输出示例

当搜索到多个候选表时，展示评分明细：

```
检索到 3 张候选表包含字段 "loan_amt"：

┌─────────────────────────────────────────────────────────────────────┐
│ 排名 │ 表名                        │ 总分 │ 评分明细                │
├─────────────────────────────────────────────────────────────────────┤
│ 1 ⭐ │ dm.dmm_sac_loan_prod_daily  │ 78   │ 口径+40, 粒度+30, 分层+8│
│ 2    │ dws.dws_loan_daily          │ 45   │ 粒度+30, 分层+15        │
│ 3    │ dwd.dwd_loan_detail         │ 23   │ 粒度+15, 分层+8         │
└─────────────────────────────────────────────────────────────────────┘

最高分与次高分差距 33 分（>10），自动选择: dm.dmm_sac_loan_prod_daily

选择理由:
• 该表已在指标库注册，口径为"当日放款总金额，单位：元"
• 粒度为 (product_code, dt)，与查询需求完全匹配
```

### 需要用户确认的场景

| 场景 | 触发条件 | 提示语 |
|------|---------|--------|
| **口径冲突** | 指标库指定表 A，但用户搜索词更接近表 B | "指标库推荐 A 表，但您搜索的词在 B 表也有匹配，请确认口径" |
| **分数接近** | Top 1 与 Top 2 分差 < 10 | "两张候选表分数接近，请选择：[A] xxx [B] yyy" |
| **粒度无法判断** | 候选表缺少粒度元数据 | "无法自动判断粒度，请确认表 X 的粒度是否为 (a, b)?" |
| **跨业务域** | 候选表属于不同业务域 | "候选表跨业务域（贷前 vs 贷后），请确认取数场景" |

### 与 generate-etl-sql 的协作

`generate-etl-sql` 在 **Step 1: 源表识别** 阶段调用本策略：

```
generate-etl-sql Step 1: 解析输入
    ↓
识别目标字段列表 + 目标表粒度
    ↓
调用 search-hive-metadata 的多源消歧策略
    ↓
获取最优源表
    ↓
继续 Step 2: 分析加工模式
```

在 `generate-etl-sql` 的输出中，标注数据来源决策：

```sql
-- ============================================================
-- 数据来源决策
-- 字段 loan_amt 候选表: dwd.dwd_loan_detail(23分), dws.dws_loan_daily(45分)
-- 选择: dws.dws_loan_daily (分数最高，粒度匹配，分层优先)
-- ============================================================
```

---

## 指标生命周期（闭环）

两个工具构成闭环：`search_existing_indicators`（查）→ `register_indicator`（入）。

- **需求阶段**: 先查指标库，找到 → 复用，未找到 → 新建开发
- **开发完成**: `generate-etl-sql` 自动识别新指标，询问用户是否注册为公共指标
- **入库判断**: dm 层通用口径 → 建议入库；da 层或特殊条件 → 询问用户

## References

- [references/metadata-schema.md](references/metadata-schema.md) - 元数据表结构详解
- [references/search-examples.md](references/search-examples.md) - 搜索示例和最佳实践
