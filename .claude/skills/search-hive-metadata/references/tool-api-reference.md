# MCP 工具 API 参考 (Tool API Reference)

> 本文件是 `search-hive-metadata` 所有 MCP 工具的完整参数/返回值文档。
> SKILL.md 中仅保留工具速览表，详细用法引用本文件。

---

## 查询类工具

### search_word_root

搜索词根表 (`word_root_dict`)，获取标准缩写用于字段命名。在 `generate-standard-ddl` 生成 DDL 时必须调用此工具。

**参数:**
- `keyword` (string, required): 搜索关键词，支持中文或英文，如"金额"、"amt"、"还款"
- `tag` (string, optional): 词根分类标签筛选，可选值：`BIZ_ENTITY`(业务实体)、`CATEGORY_WORD`(分类词)、`BOOL`(布尔)、`CONVERGE`(聚合)、`TIME`(时间)
- `match_mode` (string, optional): 匹配排序模式。`exact_first`（默认，精确/前缀优先）或 `fuzzy_only`（按字典序）
- `limit` (integer, optional): 返回结果数量，默认 20

**返回:**
- `english_abbr`: 英文缩写（**用于字段命名**）
- `chinese_name`: 中文名称
- `english_name`: 英文全称
- `alias`: 别名（可为空）
- `tag`: 分类标签
- `match_level`: 匹配层级（`exact` / `prefix` / `fuzzy` / `prefetch`）
- `score`: 匹配评分（用于排序与消歧）

**示例:**
```
搜索 "金额" → english_abbr=amt, chinese_name=金额, english_name=amount, tag=CATEGORY_WORD
搜索 "是否" → english_abbr=is, chinese_name=是否, english_name=is, tag=BOOL
按标签浏览 → search_word_root(keyword="", tag="CONVERGE") 查看所有聚合类词根
```

---

### search_word_root_batch (批量查询，优先使用)

批量查询多个语义单元的词根，最多 2 次 DB 查询（精确匹配 + 模糊回退），替代 N 次 `search_word_root` 并行调用。在 `generate-standard-ddl` Step 3.2 中优先使用。

**参数:**
- `keywords` (array[string], required): 待查询的语义单元列表，如 `["当日", "汇总", "放款", "金额"]`
- `limit_per_keyword` (integer, optional): 每个关键词返回的最大候选数，默认 5

**返回:**
- `results`: 字典，key 为关键词，value 为该关键词的候选词根列表（每项含 `english_abbr`, `chinese_name`, `english_name`, `alias`, `tag`, `match_level`）
- `total_keywords`: 输入关键词总数
- `matched_keywords`: 有匹配结果的关键词数
- `unmatched_keywords`: 无匹配结果的关键词列表

**示例:**
```json
search_word_root_batch({
  "keywords": ["今天", "汇总", "放款", "金额", "计数"],
  "limit_per_keyword": 3
})
```

**返回示例:**
```json
{
  "results": {
    "今天": [{"english_abbr": "today", "chinese_name": "今天", "tag": "TIME", "match_level": "exact"}],
    "汇总": [{"english_abbr": "sum", "chinese_name": "汇总", "tag": "CONVERGE", "match_level": "exact"}],
    "放款": [{"english_abbr": "loan", "chinese_name": "放款/贷款", "tag": "BIZ_ENTITY", "match_level": "exact"}],
    "金额": [{"english_abbr": "amt", "chinese_name": "金额", "tag": "CATEGORY_WORD", "match_level": "exact"}],
    "计数": [{"english_abbr": "cnt", "chinese_name": "件数/笔数", "tag": "CONVERGE", "match_level": "exact"}]
  },
  "total_keywords": 5,
  "matched_keywords": 5,
  "unmatched_keywords": []
}
```

**与 search_word_root 的关系:**
- `search_word_root_batch` 使用 2-phase 策略：精确匹配 `= ANY(...)` → 模糊回退 `LIKE`，最多 2 次 DB 查询
- 当语义单元 ≥ 3 个时优先使用批量版本；批量工具不可用时降级为并行调用 `search_word_root`

---

### validate_field_name

校验字段名是否严格按词根表拼接。用于 DDL 输出前的强制闸门。

**参数:**
- `field_name` (string, required): 待校验字段名，如 `today_sum_loan_amt`
- `expected_units` (array, optional): 命名证据表映射，**顺序必须与字段名中 token 出现顺序一致**。每项包含：
  - `semantic_unit` (string, optional): 最小语义单元中文
  - `english_abbr` (string, required): 选中的词根缩写（支持含下划线的复合词根，如 `cur_mon`）
  - `tag` (string, optional): 期望 tag（建议传入）
- `allow_same_tag_multi` (boolean, optional): 是否允许同一 tag 多次出现，默认 `true`

**返回:**
- `is_valid`: 是否通过校验
- `tokens`: 字段名拆词结果（含 tag/命中状态）
- `expected_units_check`: 证据覆盖校验结果
- `violations`: 违规列表（不存在词根、tag 顺序错误、证据不一致等）

**示例:**
```json
validate_field_name({
  "field_name": "today_sum_loan_amt",
  "expected_units": [
    {"semantic_unit": "今天", "english_abbr": "today", "tag": "TIME"},
    {"semantic_unit": "汇总", "english_abbr": "sum", "tag": "CONVERGE"},
    {"semantic_unit": "放款", "english_abbr": "loan", "tag": "BIZ_ENTITY"},
    {"semantic_unit": "金额", "english_abbr": "amt", "tag": "CATEGORY_WORD"}
  ]
})
```

---

### validate_field_names (批量校验，优先使用)

批量校验多个字段名的词根存在性、tag 顺序、命名证据覆盖，并检测跨字段重名。DDL 输出前优先使用此工具，降级时再逐字段调用 `validate_field_name`。

**参数:**
- `fields` (array, required): 待校验字段列表，每项包含：
  - `field_name` (string, required): 字段名
  - `expected_units` (array, optional): 命名证据列表（同 `validate_field_name`），每项含 `english_abbr`(必填)、`semantic_unit`(可选)、`tag`(建议填)
- `allow_same_tag_multi` (boolean, optional): 是否允许同一 tag 多次出现，默认 `true`
- `expected_field_count` (integer, optional): 期望的字段总数。传入后会校验实际提交数 vs 期望数是否一致，用于检测模型遗漏字段

**返回:**
- `all_valid`: 是否全部通过（含 count_mismatch 时为 false）
- `passed_count`: 通过字段数
- `failed_count`: 失败字段数
- `total_count`: 总字段数
- `duplicate_fields`: 重名字段列表
- `count_mismatch`: （仅当 expected_field_count 不匹配时出现）含 `expected`, `actual`, `message`
- `results`: 各字段校验详情（同 `validate_field_name` 返回格式）

**示例:**
```json
validate_field_names({
  "fields": [
    {
      "field_name": "today_sum_loan_amt",
      "expected_units": [
        {"semantic_unit": "今天", "english_abbr": "today", "tag": "TIME"},
        {"semantic_unit": "汇总", "english_abbr": "sum", "tag": "CONVERGE"},
        {"semantic_unit": "放款", "english_abbr": "loan", "tag": "BIZ_ENTITY"},
        {"semantic_unit": "金额", "english_abbr": "amt", "tag": "CATEGORY_WORD"}
      ]
    },
    {
      "field_name": "today_cnt_loan",
      "expected_units": [
        {"semantic_unit": "今天", "english_abbr": "today", "tag": "TIME"},
        {"semantic_unit": "计数", "english_abbr": "cnt", "tag": "CONVERGE"},
        {"semantic_unit": "放款", "english_abbr": "loan", "tag": "BIZ_ENTITY"}
      ]
    }
  ],
  "expected_field_count": 2
})
```

**与 validate_field_name 的关系:**
- `validate_field_names` 内部将所有字段的候选 token 合并为单次 DB 查询（O(1) vs O(n)），然后在内存中逐字段校验
- 额外提供：跨字段重名检测、通过/失败统计、字段数完整性检查
- DDL 场景默认使用批量版本；批量工具不可用时降级为并行调用单字段版本

---

### assemble_field_names (规则化拼接)

按词根 tag 顺序规则化拼接字段名。输入已解析的 units(root+tag)，按 `BOOL → TIME → CONVERGE → BIZ_ENTITY → CATEGORY_WORD` 排序后用 `_` 连接，生成确定性字段名。可选 DB 校验词根存在性和 tag 一致性。

**参数:**
- `fields` (array, required): 待组装字段列表，每项包含：
  - `cn_name` (string, optional): 中文字段名，用于标识
  - `units` (array, required): 语义单元列表（顺序无关，工具自动按 tag 排序），每项：
    - `root` (string, required): 词根缩写，如 `today`
    - `tag` (string, required): 词根 tag，枚举: `BOOL`/`TIME`/`CONVERGE`/`BIZ_ENTITY`/`CATEGORY_WORD`
- `validate` (boolean, optional): 是否查询 word_root_dict 验证词根存在性和 tag 一致性，默认 `true`

**返回:**
- `total`: 字段总数
- `all_valid`: 是否全部通过
- `assembled`: 各字段组装详情
  - `cn_name`: 中文名
  - `field_name`: 组装后的字段名
  - `units_ordered`: 按 tag 排序后的单元列表（含 `position`、`db_status`）
  - `is_valid`: 是否合法
  - `warnings`: 警告列表（`root_not_in_db`/`tag_mismatch`/`ambiguous_tag`/`empty_units`）

**示例:**

```json
assemble_field_names({
  "fields": [
    {
      "cn_name": "当日放款金额",
      "units": [
        {"root": "today", "tag": "TIME"},
        {"root": "sum", "tag": "CONVERGE"},
        {"root": "loan", "tag": "BIZ_ENTITY"},
        {"root": "amt", "tag": "CATEGORY_WORD"}
      ]
    }
  ]
})
```

返回: `{"total": 1, "all_valid": true, "assembled": [{"field_name": "today_sum_loan_amt", ...}]}`

**同 tag 内排序：** 工具保持同 tag 内的输入顺序（stable sort），模型应在 units 中按"被修饰词在前，修饰词在后"排列。

**与 validate_field_names 的关系:**
- `assemble_field_names` 负责**组装**：输入 roots+tags → 输出排序拼接后的字段名
- `validate_field_names` 负责**校验**：输入已拼接的字段名 → 输出词根拆解和违规检测
- 推荐流程：先 `assemble_field_names` 组装 → 再 `validate_field_names` 最终校验

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

## 写入类工具

### register_indicator

将新指标注册到指标库。ETL 开发完成后，对用户确认为公共指标的新指标执行入库，闭环"复用优先"流程。注册前自动检查重复，同名指标会跳过。

**参数:**
- `indicators` (array, required): 待注册的指标列表，每条包含：
  - **必填字段:**
  - `indicator_code` (string): 指标编码，如 `IDX_LOAN_001`
  - `indicator_name` (string): 业务指标名称，如 `当日放款金额`
  - `indicator_english_name` (string): 英文名/物理字段名，如 `today_loan_amt`
  - `indicator_category` (string): 指标分类: `原子指标`/`派生指标`/`复合指标`
  - `business_domain` (string): 业务域，如 `贷款`/`风控`/`营销`
  - `data_type` (string, **枚举**): 从元数据获取的物理字段类型。可选值: `TINYINT`/`SMALLINT`/`INT`/`BIGINT`/`FLOAT`/`DOUBLE`/`DECIMAL`/`STRING`/`VARCHAR`/`CHAR`/`DATE`/`TIMESTAMP`/`BOOLEAN`/`ARRAY`/`MAP`/`STRUCT`
  - `standard_type` (string, **枚举**): 标准类型。可选值: `数值类`/`日期类`/`文本类`/`枚举类`/`时间类`
  - `update_frequency` (string, **枚举**): 更新频率。可选值: `实时`/`每小时`/`每日`/`每周`/`每月`/`每季`/`每年`/`手动`
  - `status` (string, **枚举**): 状态。可选值: `启用`/`未启用`/`废弃`，默认 `启用`
  - `statistical_caliber` (string): 业务口径描述
  - `calculation_logic` (string): 取值逻辑，推荐格式: `SELECT 字段 FROM 表 WHERE 条件`；也可为计算公式
  - `data_source` (string): 数据来源表
  - **可选字段:**
  - `indicator_alias` (string): 指标别名
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
            "indicator_english_name": "today_sum_loan_amt",
            "indicator_category": "原子指标",
            "business_domain": "贷款",
            "data_type": "DECIMAL",
            "standard_type": "数值类",
            "update_frequency": "每日",
            "status": "启用",
            "statistical_caliber": "当日所有放款订单金额之和，单位：元",
            "calculation_logic": "SELECT SUM(loan_amt) FROM dwd.dwd_loan_dtl WHERE loan_date = '${stat_date}' AND status = 'SUCCESS'",
            "data_source": "ph_sac_dmm.dmm_sac_loan_prod_daily"
        },
        {
            "indicator_code": "IDX_LOAN_002",
            "indicator_name": "当日放款笔数",
            "indicator_english_name": "today_cnt_loan",
            "indicator_category": "原子指标",
            "business_domain": "贷款",
            "data_type": "BIGINT",
            "standard_type": "数值类",
            "update_frequency": "每日",
            "status": "启用",
            "statistical_caliber": "当日放款订单去重计数",
            "data_source": "ph_sac_dmm.dmm_sac_loan_prod_daily"
        }
    ],
    "created_by": "zhangsan"
})
```

---

## 血缘管理工具

### register_lineage

注册表级和字段级血缘关系。在 ETL 开发完成后自动调用（由 `generate-etl-sql` 触发），或手动注册历史 ETL 的血缘。

**参数:**
- `target_table` (string, required): 目标表完整名，如 "ph_sac_dmm.dmm_sac_loan_prod_daily"
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
  "target_table": "ph_sac_dmm.dmm_sac_loan_prod_daily",
  "source_tables": [
    {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
    {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
  ],
  "etl_logic_summary": "按产品维度聚合当日放款",
  "column_lineage": [
    {
      "target_column": "today_sum_loan_amt",
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
- `table_name` (string, required): 表完整名，如 "ph_sac_dmm.dmm_sac_loan_prod_daily"
- `depth` (integer, optional): 递归深度，1=仅直接依赖，2=包含二级依赖，默认 1
- `include_columns` (boolean, optional): 是否包含字段级血缘，默认 false

**返回:**
- `upstream_tables`: 上游表列表（按深度分组）
- `column_lineage`: 字段级血缘（如果 include_columns=true）
- `total_upstream`: 上游表总数

**返回示例:**
```
## 上游血缘: `ph_sac_dmm.dmm_sac_loan_prod_daily`

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

**返回示例:**
```
## 下游影响: `dwd.dwd_loan_detail`

找到 **5** 个下游表会受影响:

### 第 1 层影响
| 下游表 | JOIN 类型 | 逻辑摘要 |
|-------|----------|----------|
| `ph_sac_dmm.dmm_sac_loan_prod_daily` | FROM | 按产品维度聚合当日放款明细 |
| `ph_sac_dmm.dmm_sac_loan_chn_daily` | FROM | 按渠道维度聚合当日放款明细 |
| `dws.dws_cust_loan_summary` | FROM | 按客户维度汇总贷款信息 |

### 第 2 层影响
| 下游表 | JOIN 类型 | 逻辑摘要 |
|-------|----------|----------|
| `ph_sac_da.da_loan_report` | FROM | 汇总产品维度指标到报表层 |

**⚠️ 变更提醒**: 修改此表前，请评估对上述下游表的影响，并通知相关负责人。
```
