# Reverse Engineer Metadata - 使用指南

## 快速开始

### 场景 1: 分析单个 SQL 脚本

```bash
/reverse-engineer-metadata .claude/skills/reverse-engineer-metadata/examples/sample_dm_loan_daily.sql
```

**预期输出**:
- 表级血缘: `ph_sac_dmm.dmm_sac_loan_prod_daily` ← 3 张源表
- 字段级血缘: 10 个字段的来源追踪
- 识别指标: 8 个计算指标（金额、数量、比率）

---

### 场景 2: 批量分析整个目录

```bash
/reverse-engineer-metadata --scan sql/hive/etl/
```

**适用场景**:
- 接手遗留项目，需要快速盘点现有资产
- 元数据库缺失，需要重建血缘关系
- 数据治理专项，补齐元数据

---

### 场景 3: 仅生成报告不入库（试运行）

```bash
/reverse-engineer-metadata --scan sql/hive/etl/ --dry-run
```

**输出文件**:
- `analysis_results/dm_loan_daily_lineage.json`
- `analysis_results/dm_loan_daily_indicators.json`

---

## 工作流详解

### Phase 1: SQL 脚本解析

```
┌─ 读取 SQL 文件
├─ 去除注释和空白符
├─ 识别 INSERT OVERWRITE / INSERT INTO 目标表
├─ 提取 FROM 和 JOIN 子句（源表列表）
└─ 分割 SELECT 字段列表
```

**支持的 SQL 方言**:
- ✅ Hive (Tez)
- ✅ Impala
- ✅ Doris
- ⚠️ Spark SQL (部分支持)

---

### Phase 2: 血缘关系提取

#### 表级血缘

| 源表                      | JOIN 类型   | 关联条件                          |
|--------------------------|------------|----------------------------------|
| dwd.dwd_loan_detail      | FROM       | -                                |
| dwd.dwd_product_info     | LEFT JOIN  | a.product_id = b.product_id      |
| dws.dws_overdue_summary  | LEFT JOIN  | a.product_id = c.product_id AND... |

#### 字段级血缘

```json
{
  "target_column": "today_loan_amt",
  "source_table": "dwd.dwd_loan_detail",
  "source_column": "loan_amount",
  "transform_type": "SUM",
  "transform_expr": "SUM(a.loan_amount)"
}
```

**转换类型枚举**:
- `DIRECT`: 直接映射
- `SUM`, `COUNT`, `AVG`, `MAX`, `MIN`: 聚合函数
- `CASE`: 条件表达式
- `CUSTOM`: 复杂计算（算术运算、窗口函数）

---

### Phase 3: 指标识别

#### 自动识别规则

| 字段名模式          | 推断为         | 示例                  |
|-------------------|---------------|----------------------|
| `*_amt`, `*_amount`| 金额类指标     | today_loan_amt          |
| `*_cnt`, `*_count` | 数量类指标     | today_loan_cnt          |
| `*_rate`, `*_pct`  | 比率类指标     | success_rate         |
| `avg_*`, `mean_*`  | 平均值指标     | avg_loan_amt         |

#### 指标分类逻辑

```python
if transform_type in ('SUM', 'COUNT', 'AVG', 'MAX', 'MIN'):
    indicator_category = '原子指标'  # 直接聚合
elif transform_type in ('CASE', 'CUSTOM'):
    indicator_category = '派生指标'  # 基于原子指标计算
else:
    indicator_category = '复合指标'  # 跨表关联后计算
```

---

### Phase 4: 用户交互确认

#### 交互式 UI（伪代码）

```markdown
## 📊 分析结果

### 1️⃣ 表级血缘
[表格展示]

### 2️⃣ 字段级血缘
[折叠表格，默认展示前 10 行]

### 3️⃣ 识别的指标

| 指标英文名       | 指标中文名（待确认） | 计算逻辑              | 标准类型 |
|-----------------|---------------------|----------------------|---------|
| today_loan_amt     | 当日[loan][amt]     | SUM(loan_amount)      | 金额    |
| today_loan_cnt     | 当日[loan][cnt]     | COUNT(DISTINCT ...)   | 数量    |
| success_rate    | [success][rate]     | ... * 100.0 / ...     | 比率    |

> ⚠️ 中文名由系统推断，**请务必核对修正**！
> 部分术语未找到词根映射，用 `[英文]` 标注。

---

### ✅ 确认操作

请选择下一步操作：

- [ ] **确认入库** - 所有信息准确，执行批量注册
- [ ] **修改指标** - 需要修正中文名或业务口径
- [ ] **补充血缘** - 有遗漏的源表或字段
- [ ] **仅保存报告** - 暂不入库，导出 JSON 文件
```

#### 修改指标交互（如果用户选择 "修改指标"）

```
请输入要修改的指标英文名（如 today_loan_amt）：
> today_loan_amt

当前信息：
  中文名: 当日[loan][amt]
  业务口径: SUM(loan_amount)
  标准类型: 金额
  指标分类: 原子指标
  业务域: 贷款

请输入新的中文名（留空保持不变）：
> 当日放款金额

请输入业务口径描述（留空保持不变）：
> 统计当日所有成功放款的金额总和（不含失败件）

✅ 修改已保存。是否继续修改其他指标？[y/N]
```

---

### Phase 5: 批量入库

#### 入库前检查

1. **指标去重检查**
   ```python
   existing = search_existing_indicators(metric_name='当日放款金额')
   if existing:
       print(f"⚠️ 发现同名指标: {existing['indicator_code']}")
       print(f"   现有口径: {existing['statistical_caliber']}")
       print("   是否覆盖？[y/N]")
   ```

2. **血缘冲突检查**
   ```python
   existing_lineage = search_lineage_upstream(table_name='ph_sac_dmm.dmm_sac_loan_prod_daily')
   if existing_lineage:
       print(f"⚠️ 该表已有血缘记录，新记录将追加（不覆盖）")
   ```

#### 执行入库

```python
# 1. 注册表级血缘
register_lineage(
    target_table='ph_sac_dmm.dmm_sac_loan_prod_daily',
    source_tables=[
        {'source_table': 'dwd.dwd_loan_detail', 'join_type': 'FROM'},
        {'source_table': 'dwd.dwd_product_info', 'join_type': 'LEFT JOIN', ...},
        ...
    ],
    etl_logic_summary='关联 3 张源表加工，计算 8 个聚合指标',
    etl_script_path='sql/hive/etl/dm_loan_daily.sql'
)

# 2. 注册字段级血缘
register_lineage(
    target_table='ph_sac_dmm.dmm_sac_loan_prod_daily',
    source_tables=[...],  # 同上
    column_lineage=[
        {
            'target_column': 'today_loan_amt',
            'source_table': 'dwd.dwd_loan_detail',
            'source_column': 'loan_amount',
            'transform_type': 'SUM',
            'transform_expr': 'SUM(a.loan_amount)'
        },
        ...
    ]
)

# 3. 批量注册指标
register_indicator(
    indicators=[
        {
            'indicator_code': 'IDX_LOAN_001',  # 自动生成
            'indicator_name': '当日放款金额',
            'indicator_english_name': 'today_loan_amt',
            'calculation_logic': 'SUM(loan_amount)',
            'statistical_caliber': '统计当日所有成功放款的金额总和',
            'business_domain': '贷款',
            'standard_type': '金额',
            'indicator_category': '原子指标',
            'data_type': 'DECIMAL(20,2)',
            'update_frequency': '日',
            'data_source': 'ph_sac_dmm.dmm_sac_loan_prod_daily'
        },
        ...
    ]
)
```

#### 成功日志

```
✅ 入库完成

📊 统计信息:
  - 表级血缘: 1 条
  - 字段级血缘: 10 条
  - 新增指标: 8 条

📁 输出文件:
  - analysis_results/dm_loan_daily_lineage.json
  - analysis_results/dm_loan_daily_indicators.json
  - analysis_results/registration_log_20260208_143022.txt

🔍 验证:
  - 血缘查询: /search-lineage-upstream ph_sac_dmm.dmm_sac_loan_prod_daily
  - 指标查询: /search-existing-indicators 当日放款金额
```

---

## 高级用法

### 参数组合

```bash
# 仅提取血缘，跳过指标识别（性能优化）
/reverse-engineer-metadata --scan sql/hive/etl/ --skip-indicators

# 仅提取指标，跳过血缘分析（适用于简单聚合表）
/reverse-engineer-metadata script.sql --skip-lineage

# 试运行 + 仅血缘（最快）
/reverse-engineer-metadata --scan sql/hive/etl/ --dry-run --skip-indicators
```

### 过滤规则

```bash
# 仅分析 dm 层表
/reverse-engineer-metadata --scan sql/hive/etl/ --filter "dm_*.sql"

# 排除临时表
/reverse-engineer-metadata --scan sql/hive/etl/ --exclude "tmp_*,test_*"
```

---

## 技术限制与已知问题

### 1. SQL 解析精度

| 场景                        | 支持程度 | 备注                              |
|----------------------------|---------|----------------------------------|
| 简单 JOIN                   | ✅ 完全  | -                                |
| 多层嵌套子查询               | ⚠️ 部分  | 建议手工展开后分析                |
| WITH 子句（CTE）            | ⚠️ 部分  | 当前版本需手工展开                |
| 动态 SQL（变量替换）         | ❌ 不支持| 需用户提供替换后的 SQL            |
| `SELECT *`                  | ⚠️ 需MCP | 需调用 `list_columns` 补全        |
| 窗口函数                    | ✅ 识别  | 记录为 CUSTOM 类型                |

### 2. 中文名推断准确性

当前使用**硬编码映射表**，覆盖常见词根约 50 个。

**改进方向**:
- [ ] 集成 `search_word_root` MCP 工具，动态查询词根表
- [ ] 使用 LLM 推断复杂字段的业务含义

### 3. 指标编码自动生成

当前规则: `IDX_{业务域缩写}_{序号}`

示例: `IDX_LOAN_001`, `IDX_LOAN_002`

**改进方向**:
- [ ] 从元数据库查询现有最大编号，自动递增
- [ ] 支持用户自定义编码规则

---

## 与其他 Skill 的协作

### 1. 配合 `review-sql` 使用

```
工作流:
  1. /reverse-engineer-metadata --scan sql/hive/etl/  # 批量分析
  2. /review-sql sql/hive/etl/dm_loan_daily.sql      # 审查规范性
  3. 根据审查报告修正 SQL
  4. 重新分析并入库
```

### 2. 配合 `dw-dev-workflow` 使用

```
场景: 遗留系统改造

  Phase 1: /reverse-engineer-metadata --scan legacy/  # 盘点现有资产
  Phase 2: 导出指标清单，识别冗余和缺失
  Phase 3: /dw-requirement-triage <重构需求文档>      # 规划新架构
  Phase 4: /generate-standard-ddl ...                 # 重建标准化表
  Phase 5: /generate-etl-sql ...                      # 重写 ETL
```

---

## 输出文件说明

### 1. `*_lineage.json`

```json
{
  "target_table": "ph_sac_dmm.dmm_sac_loan_prod_daily",
  "sql_file": "sql/hive/etl/dm_loan_daily.sql",
  "table_lineage": [
    {
      "source_table": "dwd.dwd_loan_detail",
      "join_type": "FROM",
      "join_condition": null
    },
    ...
  ],
  "column_lineage": [
    {
      "target_column": "today_loan_amt",
      "source_table": "dwd.dwd_loan_detail",
      "source_column": "loan_amount",
      "transform_type": "SUM",
      "transform_expr": "SUM(a.loan_amount)"
    },
    ...
  ],
  "etl_logic_summary": "关联 3 张源表加工，计算 8 个聚合指标"
}
```

### 2. `*_indicators.json`

```json
{
  "indicators": [
    {
      "indicator_code": "IDX_LOAN_001",
      "indicator_name": "当日放款金额",
      "indicator_english_name": "today_loan_amt",
      "calculation_logic": "SUM(loan_amount)",
      "statistical_caliber": "统计当日所有成功放款的金额总和",
      "business_domain": "贷款",
      "standard_type": "金额",
      "indicator_category": "原子指标",
      "data_type": "DECIMAL(20,2)",
      "update_frequency": "日",
      "data_source": "ph_sac_dmm.dmm_sac_loan_prod_daily"
    },
    ...
  ]
}
```

### 3. `registration_log.txt`

```
================================================================================
Reverse Engineering Registration Log
Generated at: 2026-02-08 14:30:22
================================================================================

[INFO] Starting analysis for: sql/hive/etl/dm_loan_daily.sql
[INFO] Target table detected: ph_sac_dmm.dmm_sac_loan_prod_daily
[INFO] Found 3 source tables
[INFO] Extracted 10 column lineages
[INFO] Identified 8 indicators

[CHECK] Checking for existing indicators...
[WARN] Indicator "当日放款金额" already exists (IDX_LOAN_001)
[PROMPT] User chose to: SKIP

[API] Calling register_lineage...
[SUCCESS] Table lineage registered

[API] Calling register_lineage (column level)...
[SUCCESS] Column lineage registered (10 entries)

[API] Calling register_indicator...
[SUCCESS] Indicators registered (7 new, 1 skipped)

[DONE] Registration completed
================================================================================
```

---

## FAQ

### Q1: 解析失败怎么办？

**A**: 检查以下情况：
1. SQL 语法是否正确（可先在 Hive/Impala 中执行验证）
2. 是否包含不支持的动态 SQL 或宏
3. 尝试使用 `--dry-run` 查看详细错误日志

### Q2: 中文名推断不准确怎么修正？

**A**: 两种方式：
1. 交互式修正：在 Phase 4 选择 "修改指标"
2. 手工编辑 JSON：修改 `*_indicators.json` 后，使用 `--from-json` 参数入库

### Q3: 如何避免重复入库？

**A**: Skill 内置去重检查：
- 指标：按 `indicator_name` 和 `indicator_code` 检查
- 血缘：按 `target_table + source_table` 组合检查
- 用户可选择 "跳过" 或 "覆盖"

### Q4: 支持哪些 SQL 引擎？

**A**:
- ✅ **完全支持**: Hive (Tez), Impala
- ⚠️ **部分支持**: Doris (INSERT INTO 语法), Spark SQL (部分 UDF 不识别)
- ❌ **不支持**: PostgreSQL, MySQL（语法差异过大）

---

## 版本历史

- **v1.0** (2026-02-08): 初始版本
  - 支持 Hive/Impala/Doris SQL 解析
  - 表级和字段级血缘提取
  - 指标自动识别（SUM/COUNT/AVG 等）
  - 交互式确认和批量入库

---

**Created by**: Claude Code
**Maintainer**: Data Warehouse Team
**Last Updated**: 2026-02-08
