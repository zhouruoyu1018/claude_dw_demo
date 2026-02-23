# reverse-engineer-metadata

**逆向工程：从存量 SQL 脚本提取指标和血缘**

## 使用场景

- 接手遗留数仓项目，需要补齐元数据
- 存量 ETL 脚本缺少文档，需要自动化分析
- 元数据库缺失，需要从代码反推血缘关系
- 数据治理时需要盘点现有指标资产

## 核心能力

1. **血缘提取**
   - 表级依赖：目标表 ← 源表列表（含 JOIN 类型）
   - 字段级映射：目标字段 ← 源表.源字段 + 转换逻辑

2. **指标识别**
   - 聚合指标：SUM/COUNT/AVG/MAX/MIN
   - 派生指标：CASE WHEN、算术运算、窗口函数
   - 比率指标：除法运算、ROUND 包装

3. **智能推断**
   - 根据字段名和计算逻辑推断 `standard_type`（数值类/日期类/文本类/枚举类/时间类）
   - 根据 SQL 位置推断 `update_frequency`（分区字段 = dt 则为 `每日`）
   - 根据表前缀推断 `business_domain`（dm_loan → 贷款域）

4. **交互确认**
   - 展示解析结果（表级血缘 + 字段级映射 + 指标清单）
   - 支持用户修正/补充信息
   - 批量入库前二次确认

## 工作流

```
┌─────────────────────┐
│  Phase 1: 脚本解析  │
│  - 读取多个 SQL 文件 │
│  - 识别目标表        │
│  - 提取源表列表      │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Phase 2: 血缘分析  │
│  - 表级依赖 (JOIN)  │
│  - 字段级映射 (投影) │
│  - 转换逻辑提取      │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Phase 3: 指标识别  │
│  - 聚合函数检测      │
│  - 指标命名对齐      │
│  - 业务口径推断      │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Phase 4: 用户交互  │
│  - 展示分析结果      │
│  - 收集确认/修正     │
│  - 生成入库 Payload  │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Phase 5: 批量入库  │
│  - register_lineage │
│  - register_indicator│
│  - 输出成功日志      │
└─────────────────────┘
```

## 调用方式

### 方式 1: 指定脚本路径

```
/reverse-engineer-metadata sql/hive/etl/dm_loan_daily.sql sql/hive/etl/dm_overdue_summary.sql
```

### 方式 2: 扫描整个目录

```
/reverse-engineer-metadata --scan sql/hive/etl/
```

### 方式 3: 交互式选择

```
/reverse-engineer-metadata
```
然后 Claude 会：
1. 列出项目中所有 DML 脚本
2. 让用户多选需要分析的文件
3. 执行分析流程

## 参数说明

- `--scan <directory>`: 扫描目录下所有 `.sql` 文件
- `--dry-run`: 仅分析不入库，输出 JSON 结果
- `--skip-indicators`: 仅提取血缘，跳过指标识别
- `--skip-lineage`: 仅提取指标，跳过血缘分析

## 输出示例

### 阶段输出

```markdown
## 📊 分析结果

### 1️⃣ 表级血缘

目标表: `ph_sac_dmm.dmm_sac_loan_prod_daily`

| 源表                         | JOIN 类型    | 关联条件                              |
|------------------------------|-------------|--------------------------------------|
| dwd.dwd_loan_detail          | FROM        | -                                    |
| dwd.dwd_product_info         | LEFT JOIN   | a.product_id = b.product_id          |
| dws.dws_overdue_summary      | LEFT JOIN   | a.loan_id = c.loan_id AND c.dt = a.dt|

### 2️⃣ 字段级血缘（部分展示）

| 目标字段         | 源表.源字段                     | 转换逻辑                      | 类型     |
|-----------------|--------------------------------|------------------------------|---------|
| dt              | dwd_loan_detail.dt             | DIRECT                       | DIRECT  |
| product_id      | dwd_loan_detail.product_id     | DIRECT                       | DIRECT  |
| td_loan_amt     | dwd_loan_detail.loan_amount    | SUM(loan_amount)             | SUM     |
| td_loan_cnt     | dwd_loan_detail.loan_id        | COUNT(DISTINCT loan_id)      | COUNT   |
| avg_loan_amt    | -                              | td_loan_amt / td_loan_cnt    | CUSTOM  |

### 3️⃣ 识别的指标

| 指标英文名        | 指标中文名      | 计算逻辑                       | 标准类型  | 分类     |
|------------------|----------------|-------------------------------|---------|---------|
| td_loan_amt      | 当日放款金额    | SUM(loan_amount)              | 数值类   | 原子指标 |
| td_loan_cnt      | 当日放款笔数    | COUNT(DISTINCT loan_id)       | 数值类   | 原子指标 |
| avg_loan_amt     | 平均单笔金额    | td_loan_amt / td_loan_cnt     | 数值类   | 派生指标 |

---

### ✅ 请确认

1. **指标信息准确吗？** 是否需要修正中文名或业务口径？
2. **血缘关系完整吗？** 是否有遗漏的源表或字段？
3. **确认后执行入库？**

[✓ 确认入库] [✗ 取消] [✎ 修改指标] [+ 补充血缘]
```

## 技术实现要点

### SQL 解析策略

```python
# 伪代码示例
def parse_insert_overwrite_sql(sql_content):
    # 1. 提取目标表
    target_table = re.search(r'INSERT OVERWRITE TABLE\s+(\S+)', sql_content, re.I)

    # 2. 提取源表（FROM + JOIN）
    from_clause = re.search(r'FROM\s+(\S+)', sql_content, re.I)
    join_clauses = re.findall(r'(LEFT|RIGHT|INNER|FULL)?\s*JOIN\s+(\S+)\s+ON\s+([^WHERE|GROUP|ORDER]+)', sql_content, re.I)

    # 3. 提取 SELECT 列表
    select_clause = re.search(r'SELECT\s+(.*?)\s+FROM', sql_content, re.I | re.DOTALL)

    # 4. 分析每个字段的血缘
    for field_expr in select_clause.split(','):
        # 识别别名
        alias = re.search(r'AS\s+(\w+)', field_expr, re.I)

        # 识别聚合函数
        if re.search(r'SUM\(', field_expr, re.I):
            transform_type = 'SUM'
        elif re.search(r'COUNT\(', field_expr, re.I):
            transform_type = 'COUNT'
        # ... 其他类型

    return {
        'target_table': target_table,
        'source_tables': [...],
        'column_lineage': [...],
        'indicators': [...]
    }
```

### 指标识别规则

| 模式                          | 识别为          | standard_type |
|-------------------------------|----------------|---------------|
| `SUM(xxx_amt)`               | 聚合指标        | 数值类         |
| `COUNT(DISTINCT xxx_id)`     | 去重计数指标     | 数值类         |
| `SUM(CASE WHEN ... 1 ELSE 0)`| 条件计数指标     | 数值类         |
| `A / NULLIF(B, 0)`           | 比率类派生指标   | 数值类         |
| `ROW_NUMBER() OVER (...)`    | 排名类指标（不入库）| -           |
| `DATE/TIMESTAMP 字段`         | 日期指标        | 日期类/时间类   |
| `CASE WHEN → 有限枚举值`      | 枚举指标        | 枚举类         |
| `VARCHAR/STRING 文本输出`      | 文本指标        | 文本类         |

### MCP 工具映射

| 分析阶段         | MCP 工具                   | 用途                          |
|-----------------|----------------------------|------------------------------|
| Phase 2         | `get_table_detail`         | 获取目标表现有字段列表        |
| Phase 3         | `search_existing_indicators`| 检查指标是否已注册           |
| Phase 5         | `register_lineage`         | 批量注册表级和字段级血缘      |
| Phase 5         | `register_indicator`       | 批量注册新识别的指标          |

## 与其他 Skill 的关系

```
dw-requirement-triage    ──→  生成新需求的指标和血缘（前向）
                              ↑ 可对比复用检查
reverse-engineer-metadata ──→ 从存量代码提取指标和血缘（逆向）
                              ↑ 补齐元数据缺口
```

**互补场景**:
- 新项目启动：`dw-requirement-triage` + `generate-standard-ddl` + `generate-etl-sql`
- 遗留系统接手：`reverse-engineer-metadata` 批量盘点 → 然后用 `review-sql` 审查规范性

## 注意事项

1. **SQL 方言差异**
   - Hive: 支持 `INSERT OVERWRITE TABLE ... PARTITION (...)`
   - Impala: 支持 `INSERT INTO ... SELECT`
   - Doris: 支持 `INSERT INTO ... VALUES` 和 Stream Load
   - 需要识别引擎类型（根据文件路径或 SQL 特征）

2. **复杂 SQL 处理**
   - 多层嵌套子查询：递归解析
   - WITH 子句（CTE）：先展开再分析
   - 动态 SQL（变量替换）：提示用户手动补充

3. **字段级血缘精度**
   - `SELECT *` 场景：调用 `list_columns` 获取完整字段列表
   - 复杂表达式：记录为 `CUSTOM` 类型，保留原始 SQL 表达式

4. **指标去重**
   - 调用 `search_existing_indicators` 检查是否已注册
   - 如果存在同名指标，展示差异让用户决定是否覆盖

## 输出文件

执行完成后生成：
- `analysis_results/<target_table>_lineage.json` - 血缘关系 JSON
- `analysis_results/<target_table>_indicators.json` - 指标清单 JSON
- `analysis_results/registration_log.txt` - 入库日志

## 示例调用

```bash
# 场景 1: 分析单个脚本
/reverse-engineer-metadata sql/hive/etl/dm_loan_daily.sql

# 场景 2: 批量分析 dm 层所有表
/reverse-engineer-metadata --scan sql/hive/etl/ --filter "dm_*.sql"

# 场景 3: 仅生成分析报告不入库
/reverse-engineer-metadata --scan sql/hive/etl/ --dry-run

# 场景 4: 仅提取血缘（性能优化场景）
/reverse-engineer-metadata sql/hive/etl/dm_loan_daily.sql --skip-indicators
```

## 扩展能力

未来可增强：
- [ ] 支持 Python/Spark SQL 脚本解析
- [ ] 与 Git 历史集成，追踪字段变更历史
- [ ] 生成 Mermaid 血缘图可视化
- [ ] 与调度系统集成，自动检测新增脚本

---

**Created by**: Claude Code
**Version**: 1.0
**Last Updated**: 2026-02-08
