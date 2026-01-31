# 数据血缘管理功能使用指南

## 概述

数据血缘功能已集成到 `search-hive-metadata` MCP Server 中，提供表级和字段级血缘的注册与查询能力。

## 数据库表结构

### 表级血缘 (data_lineage)

存储表与表之间的依赖关系。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 主键 |
| target_table | VARCHAR(200) | 目标表完整名 |
| source_table | VARCHAR(200) | 源表完整名 |
| join_type | VARCHAR(50) | JOIN 类型 (FROM/LEFT JOIN/INNER JOIN 等) |
| relation_type | VARCHAR(50) | 关系类型 (ETL/VIEW/MANUAL) |
| etl_script_path | VARCHAR(500) | ETL 脚本路径 |
| etl_logic_summary | VARCHAR(1000) | ETL 逻辑摘要 |
| is_active | BOOLEAN | 是否有效 |

### 字段级血缘 (column_lineage)

存储字段与字段之间的依赖关系。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 主键 |
| target_table | VARCHAR(200) | 目标表 |
| target_column | VARCHAR(200) | 目标字段 |
| source_table | VARCHAR(200) | 源表 |
| source_column | VARCHAR(200) | 源字段 |
| transform_type | VARCHAR(50) | 转换类型 (DIRECT/SUM/COUNT/AVG/MAX/MIN/CASE/CUSTOM) |
| transform_expr | VARCHAR(1000) | 转换表达式 |

---

## MCP 工具使用

### 1. register_lineage - 注册血缘关系

**使用场景**: 在 ETL 开发完成后，自动或手动注册表级和字段级血缘。

**调用示例**:

```javascript
{
  "tool": "register_lineage",
  "arguments": {
    "target_table": "dm.dmm_sac_loan_prod_daily",
    "source_tables": [
      {
        "source_table": "dwd.dwd_loan_detail",
        "join_type": "FROM"
      },
      {
        "source_table": "dim.dim_product",
        "join_type": "LEFT JOIN"
      }
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
}
```

**返回示例**:

```
## 血缘注册结果

**目标表**: `dm.dmm_sac_loan_prod_daily`

### 表级血缘

| 操作 | 源表 | JOIN 类型 |
|-----|------|----------|
| 新增 | `dwd.dwd_loan_detail` | FROM |
| 新增 | `dim.dim_product` | LEFT JOIN |

### 字段级血缘

| 操作 | 目标字段 | 来源 |
|-----|---------|------|
| 新增 | `td_sum_loan_amt` | `dwd.dwd_loan_detail.loan_amount` |

**汇总**: 表级血缘 2 条，字段级血缘 1 条
```

---

### 2. search_lineage_upstream - 查询上游依赖

**使用场景**:
- 了解数据来源
- 追溯数据质量问题
- 评估源表变更影响

**调用示例**:

```javascript
{
  "tool": "search_lineage_upstream",
  "arguments": {
    "table_name": "dm.dmm_sac_loan_prod_daily",
    "depth": 2,
    "include_columns": true
  }
}
```

**参数说明**:
- `table_name`: 要查询的表完整名
- `depth`: 递归深度（1=直接依赖，2=包含二级依赖）
- `include_columns`: 是否包含字段级血缘

**返回示例**:

```
## 上游血缘: `dm.dmm_sac_loan_prod_daily`

找到 **4** 个上游依赖:

### 第 1 层依赖

| 源表 | JOIN 类型 | 关系 | 逻辑摘要 |
|------|----------|------|----------|
| `dwd.dwd_loan_detail` | FROM | ETL | 按产品维度聚合当日放款明细 |
| `dim.dim_product` | LEFT JOIN | ETL | 关联产品维度获取产品名称 |

### 第 2 层依赖

| 源表 | JOIN 类型 | 关系 | 逻辑摘要 |
|------|----------|------|----------|
| `ods.ods_loan_apply` | FROM | ETL | 清洗贷款申请数据 |
| `ods.ods_loan_contract` | LEFT JOIN | ETL | 关联合同信息补充放款字段 |

### 字段级血缘

| 目标字段 | 来源表.字段 | 转换类型 | 表达式 |
|---------|------------|---------|--------|
| `product_code` | `dwd.dwd_loan_detail.product_code` | DIRECT | product_code |
| `td_sum_loan_amt` | `dwd.dwd_loan_detail.loan_amount` | SUM | SUM(loan_amount) |
```

---

### 3. search_lineage_downstream - 查询下游影响

**使用场景**:
- 评估表变更影响范围
- 通知下游用户
- 规划数据迁移
- 影响分析报告

**调用示例**:

```javascript
{
  "tool": "search_lineage_downstream",
  "arguments": {
    "table_name": "dwd.dwd_loan_detail",
    "depth": 2,
    "include_columns": false
  }
}
```

**返回示例**:

```
## 下游影响: `dwd.dwd_loan_detail`

找到 **5** 个下游表会受影响:

### 第 1 层影响

| 下游表 | JOIN 类型 | 关系 | 逻辑摘要 |
|-------|----------|------|----------|
| `dm.dmm_sac_loan_prod_daily` | FROM | ETL | 按产品维度聚合当日放款明细 |
| `dm.dmm_sac_loan_chn_daily` | FROM | ETL | 按渠道维度聚合当日放款明细 |
| `dws.dws_cust_loan_summary` | FROM | ETL | 按客户维度汇总贷款信息 |

### 第 2 层影响

| 下游表 | JOIN 类型 | 关系 | 逻辑摘要 |
|-------|----------|------|----------|
| `da.da_loan_report` | FROM | ETL | 汇总产品维度指标到报表层 |
| `da.da_loan_report` | LEFT JOIN | ETL | 关联渠道维度指标 |

**⚠️ 变更提醒**: 修改此表前，请评估对上述下游表的影响，并通知相关负责人。
```

---

## 工作流集成

### 在 generate-etl-sql 中自动注册

ETL SQL 生成完成后，会自动调用 `register_lineage` 注册血缘关系。

**Step 6: 血缘注册（自动）**

```
生成 ETL SQL
    ↓
解析 INSERT 目标表
    ↓
解析 FROM/JOIN 源表
    ↓
提取字段映射
    ↓
调用 register_lineage
    ↓
在 ETL 脚本头部添加血缘注释
```

**ETL 脚本头部血缘注释示例**:

```sql
-- ============================================================
-- 数据血缘 (Data Lineage)
-- ────────────────────────────────────────────────────────────
-- 上游依赖:
--   • dwd.dwd_loan_detail (FROM) - 放款明细
--   • dim.dim_product (LEFT JOIN) - 产品维度
-- 下游影响: 查询 search_lineage_downstream 获取
-- ============================================================
```

---

## 测试数据说明

已在 PostgreSQL 数据库 `phslm` 中创建测试数据，包含以下血缘链路：

```
ODS 层
  ods.ods_loan_apply ──┐
  ods.ods_loan_contract┘
         ↓
DWD 层
  dwd.dwd_loan_detail
         ↓
    ┌────┴────┬────────────┐
    ↓         ↓            ↓
DM/DWS 层
  dm.dmm_sac_loan_prod_daily
  dm.dmm_sac_loan_chn_daily
  dws.dws_cust_loan_summary
         ↓
DA 层
  da.da_loan_report
```

**测试数据统计**:
- 表级血缘: 9 条
- 字段级血缘: 11 条

---

## 常见问题

### Q1: 如何验证血缘数据是否正确？

**方法 1**: 使用 SQL 查询

```sql
-- 查询某表的上游
SELECT source_table, join_type, etl_logic_summary
FROM data_lineage
WHERE target_table = 'dm.dmm_sac_loan_prod_daily' AND is_active = TRUE;

-- 查询某表的下游
SELECT target_table, join_type, etl_logic_summary
FROM data_lineage
WHERE source_table = 'dwd.dwd_loan_detail' AND is_active = TRUE;
```

**方法 2**: 使用 MCP 工具

直接调用 `search_lineage_upstream` 或 `search_lineage_downstream`。

**方法 3**: 运行测试脚本

```bash
cd E:\claude_demo\.claude\skills\search-hive-metadata\scripts
python test_lineage.py
```

---

### Q2: 血缘数据何时注册？

**自动注册**:
- 在 `/dw-dev-workflow` 工作流的 Phase 4（ETL 开发）完成后自动注册

**手动注册**:
- 使用 `register_lineage` MCP 工具手动注册历史 ETL 的血缘关系

---

### Q3: 如何处理血缘冲突？

血缘注册时会检查是否已存在相同的 `target_table` + `source_table` 组合：
- **已存在**: 更新 `join_type`、`etl_logic_summary` 等字段
- **不存在**: 插入新记录

字段级血缘同理，按 `target_table` + `target_column` + `source_table` + `source_column` 判重。

---

### Q4: 如何删除无效血缘？

不建议物理删除，而是将 `is_active` 设为 `FALSE`：

```sql
UPDATE data_lineage
SET is_active = FALSE
WHERE target_table = 'old_table_name';
```

---

## 下一步优化建议

1. **血缘可视化**: 开发血缘关系图（DAG）前端展示
2. **影响分析报告**: 生成表变更影响分析报告（PDF/Excel）
3. **血缘审计**: 定期检查血缘数据完整性
4. **SQL 解析增强**: 自动从 ETL SQL 中解析血缘关系（减少手动注册）
5. **集成调度系统**: 从调度系统（如 Airflow）自动采集血缘

---

## 相关文档

- [metadata-schema.md](references/metadata-schema.md) - 血缘表结构详细说明
- [generate-etl-sql/SKILL.md](../generate-etl-sql/SKILL.md) - ETL 生成中的血缘采集
- [dw-dev-workflow/SKILL.md](../dw-dev-workflow/SKILL.md) - 完整工作流说明
