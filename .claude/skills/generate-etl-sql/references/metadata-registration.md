# 指标入库与血缘注册

本文件承接 `generate-etl-sql` 的 Step 5/Step 6 详细实现细则。

## Step 5 详细规则：指标入库

### 5.1 识别候选指标

从 ETL 目标字段中识别指标字段（排除维度字段），逐一调用 `search_existing_indicators`：

- 命中：跳过，不重复注册
- 未命中：进入候选列表

### 5.2 用户确认策略

向用户展示候选指标并确认：

- `(A) 全部注册`
- `(B) 仅注册第 N 项`
- `(C) 不注册`

建议展示字段：

- 指标中文名 / 英文名（目标字段）
- 业务口径
- 数据来源表

### 5.3 注册 Payload 规范

调用 `register_indicator` 时，每条指标需满足：

- `data_type`: 物理类型枚举（如 `DECIMAL` / `BIGINT` / `VARCHAR`）
- `standard_type`: `数值类` / `日期类` / `文本类` / `枚举类` / `时间类`
- `update_frequency`: `实时` / `每小时` / `每日` / `每周` / `每月` / `每季` / `每年` / `手动`
- `status`: `启用` / `未启用` / `废弃`（默认 `启用`）
- `calculation_logic`: 必填，建议 `SELECT 字段 FROM 表 WHERE 条件`

示例：

```json
{
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
      "calculation_logic": "SELECT SUM(loan_amt) FROM dwd.dwd_loan_dtl WHERE loan_date = '${stat_date}'",
      "data_source": "ph_sac_dmm.dmm_sac_loan_prod_daily"
    }
  ],
  "created_by": "auto"
}
```

### 5.4 失败处理

| 失败类型 | 处理方式 |
|---------|---------|
| MCP 不可用（连接失败/超时） | 输出 `-- [MCP-PENDING] register_indicator: {...}` 到 ETL 头部注释，提示后续补录 |
| 参数校验失败/编码冲突 | 展示错误；可自动修正项（如编码后缀）重试 1 次；仍失败则标记 `-- [REGISTER-FAILED]` |

无论成功与否，不阻塞后续 Step 6 / QA 流程。

### 5.5 是否建议入库

| 条件 | 建议 |
|------|------|
| dm 层通用口径指标 | 建议入库 |
| da 层一次性报表指标 | 询问用户 |
| 特殊过滤口径指标 | 询问用户并在 remarks 注明限制条件 |

## Step 6 详细规则：血缘注册

### 6.1 提取策略

从 ETL SQL 解析：

- 表级血缘：目标表、源表、JOIN 类型
- 字段级血缘（可选）：`target_column`、`source_table`、`source_column`、`transform_type`、`transform_expr`

### 6.2 transform_type 映射

| SELECT 表达式 | transform_type |
|--------------|----------------|
| `src.col` | `DIRECT` |
| `SUM(src.col)` | `SUM` |
| `COUNT(src.col)` | `COUNT` |
| `AVG(src.col)` | `AVG` |
| `MAX(src.col)` | `MAX` |
| `MIN(src.col)` | `MIN` |
| `CASE WHEN ... END` | `CASE` |
| 其他复杂表达式 | `CUSTOM` |

### 6.3 注册 Payload 规范

调用 `register_lineage`：

```json
{
  "target_table": "ph_sac_dmm.dmm_sac_loan_prod_daily",
  "source_tables": [
    {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
    {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
  ],
  "etl_script_path": "sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql",
  "etl_logic_summary": "按产品维度聚合当日放款",
  "column_lineage": [
    {
      "target_column": "td_sum_loan_amt",
      "source_table": "dwd.dwd_loan_detail",
      "source_column": "loan_amount",
      "transform_type": "SUM",
      "transform_expr": "SUM(loan_amount)"
    }
  ],
  "created_by": "auto"
}
```

### 6.4 失败处理

| 失败类型 | 处理方式 |
|---------|---------|
| MCP 不可用（连接失败/超时） | 输出 `-- [MCP-PENDING] register_lineage: {...}` 到 ETL 头部注释 |
| 参数错误/目标表不存在 | 标记 `-- [LINEAGE-FAILED]` 并附错误原因 |

无论成功与否，不阻塞 QA。

### 6.5 ETL 头部注释要求

在 ETL 头部保留血缘摘要：

- 目标表
- 上游依赖表及 JOIN 类型
- 查询下游影响的入口（`search_lineage_downstream`）

## 配套查询接口

- `search_lineage_upstream`: 查上游依赖
- `search_lineage_downstream`: 查下游影响

用于变更评估与回归范围圈定。
