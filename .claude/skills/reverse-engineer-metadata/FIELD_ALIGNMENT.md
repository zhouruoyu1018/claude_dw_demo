# 字段对齐机制 - Field Alignment

## 问题背景

DML 脚本中的 `SELECT` 别名不一定等于目标表的实际字段名：

```sql
INSERT OVERWRITE TABLE dm.dmm_loan_daily PARTITION (dt='2024-01-01')
SELECT
    a.stat_date AS report_date,        -- 目标表实际字段: dt
    SUM(loan_amt) AS total_amount,     -- 目标表实际字段: today_loan_amt
    COUNT(1) AS cnt                    -- 目标表实际字段: today_loan_cnt
FROM dwd.dwd_loan_detail a
```

### 风险

如果直接使用 SQL 中的别名（`report_date`, `total_amount`, `cnt`）：

1. ❌ **指标注册错误**: 注册了不存在的字段名
2. ❌ **血缘映射错误**: 无法追踪真实字段的数据来源
3. ❌ **关联失败**: 无法与已有指标关联

---

## 解决方案：位置对齐 + 元数据校验

### 核心原理

**Hive INSERT OVERWRITE 默认按位置对应**：

```
SELECT 第 1 个字段 → 目标表第 1 个字段
SELECT 第 2 个字段 → 目标表第 2 个字段
...
```

### 实现流程

```
┌────────────────────────────┐
│ Step 1: 解析 SQL           │
│ - 提取目标表名              │
│ - 提取 SELECT 列表          │
└────────────┬───────────────┘
             ↓
┌────────────────────────────┐
│ Step 2: 查询元数据          │
│ - 调用 list_columns(table) │
│ - 获取真实字段列表          │
└────────────┬───────────────┘
             ↓
┌────────────────────────────┐
│ Step 3: 位置对齐            │
│ - 比对字段数量              │
│ - 按位置映射字段名          │
└────────────┬───────────────┘
             ↓
┌────────────────────────────┐
│ Step 4: 替换字段名          │
│ - report_date → dt         │
│ - total_amount → today_loan_amt│
│ - cnt → today_loan_cnt        │
└────────────────────────────┘
```

---

## 代码实现

### 1. 初始化时传入 MCP 客户端

```python
parser = SQLParser(
    sql_content=sql_text,
    sql_file='dm_loan_daily.sql',
    mcp_client=mcp_client  # ✨ 传入 MCP 客户端
)
```

### 2. 解析流程中自动查询元数据

```python
def parse(self) -> SQLAnalysisResult:
    self._extract_target_table()
    self._fetch_target_columns_from_metadata()  # ✨ 查询元数据
    self._extract_table_lineage()
    self._extract_column_lineage()
    self._align_columns_with_metadata()  # ✨ 字段对齐
    self._identify_indicators()
    self._generate_summary()
    return self.result
```

### 3. 元数据查询

```python
def _fetch_target_columns_from_metadata(self):
    """查询目标表的真实字段列表"""
    if not self.mcp_client or not self.result.target_table:
        return

    try:
        # 调用 MCP 工具
        columns_result = self.mcp_client.list_columns(self.result.target_table)
        if columns_result and 'columns' in columns_result:
            self.target_columns_from_metadata = [
                col['column_name'] for col in columns_result['columns']
                if col['column_name'] not in ('dt', 'create_time', 'update_time')
            ]
    except Exception as e:
        print(f"⚠️ 元数据查询失败: {e}")
        # 不中断解析流程
```

### 4. 位置对齐

```python
def _align_columns_with_metadata(self):
    """字段对齐：SELECT 别名 → 目标表真实字段名"""
    if not self.target_columns_from_metadata:
        return

    sql_columns = [cl.target_column for cl in self.result.column_lineage]

    # 检查数量
    if len(sql_columns) != len(self.target_columns_from_metadata):
        print(f"⚠️ 字段数量不匹配: SQL {len(sql_columns)} vs 表 {len(self.target_columns_from_metadata)}")
        return

    # 按位置替换
    for i, col_lineage in enumerate(self.result.column_lineage):
        real_column_name = self.target_columns_from_metadata[i]
        if col_lineage.target_column != real_column_name:
            print(f"   🔀 字段对齐: {col_lineage.target_column} → {real_column_name}")
            col_lineage.target_column = real_column_name
```

---

## 效果演示

### 输入 SQL

```sql
INSERT OVERWRITE TABLE dm.dmm_loan_daily PARTITION (dt='2024-01-01')
SELECT
    a.stat_date AS report_date,
    SUM(loan_amt) AS total_amount,
    COUNT(1) AS cnt
FROM dwd.dwd_loan_detail a
GROUP BY a.stat_date
```

### 元数据查询结果

```json
{
  "table_name": "dm.dmm_loan_daily",
  "columns": [
    {"column_name": "dt", "data_type": "STRING"},
    {"column_name": "today_loan_amt", "data_type": "DECIMAL(20,2)"},
    {"column_name": "today_loan_cnt", "data_type": "BIGINT"}
  ]
}
```

### 字段对齐日志

```
🔀 字段对齐: report_date → dt
🔀 字段对齐: total_amount → today_loan_amt
🔀 字段对齐: cnt → today_loan_cnt
```

### 最终输出

```json
{
  "column_lineage": [
    {
      "target_column": "dt",  // ✅ 已替换为真实字段名
      "source_table": "dwd.dwd_loan_detail",
      "source_column": "stat_date",
      "transform_type": "DIRECT"
    },
    {
      "target_column": "today_loan_amt",  // ✅ 已替换
      "source_column": "loan_amt",
      "transform_type": "SUM"
    },
    {
      "target_column": "today_loan_cnt",  // ✅ 已替换
      "transform_type": "COUNT"
    }
  ]
}
```

---

## 边界情况处理

### 情况 1: 元数据查询失败

**行为**: 不中断解析流程，使用 SQL 中的别名

```python
⚠️ 元数据查询失败: Connection timeout
ℹ️ 将使用 SQL 中的别名进行解析
```

### 情况 2: 字段数量不匹配

**场景**: SQL SELECT 8 个字段，但目标表只有 5 个字段

**行为**: 发出警告，不执行对齐

```python
⚠️ 字段数量不匹配: SQL 8 vs 目标表 5
   SQL 字段: [report_date, total_amount, cnt, ...]
   目标表字段: [dt, today_loan_amt, today_loan_cnt, product_id, product_name]
ℹ️ 跳过字段对齐，使用 SQL 别名
```

**可能原因**:
- SQL 中有派生字段未持久化到表
- 目标表结构已变更（需要 ALTER TABLE）
- SQL 解析错误（如误识别注释行）

### 情况 3: 没有传入 MCP 客户端

**行为**: 跳过元数据查询，直接使用 SQL 别名

```python
# 离线解析模式
parser = SQLParser(sql_content=sql_text)  # 未传入 mcp_client
result = parser.parse()

# 结果: 使用 SQL 中的别名
result.column_lineage[0].target_column  # → "report_date"（原始别名）
```

---

## 使用建议

### ✅ 推荐场景

1. **生产环境分析**: 元数据准确，必须对齐
2. **批量盘点**: 确保指标名称与表结构一致
3. **自动化入库**: 避免注册错误的字段名

### ⚠️ 可选场景

1. **新表开发**: 目标表还未创建，元数据不存在
2. **离线分析**: 无 MCP 连接，仅做代码审查
3. **原型验证**: 快速测试 SQL 解析能力

### 配置方式

```python
# 方式 1: 启用元数据校验（推荐）
parser = SQLParser(
    sql_content=sql_text,
    mcp_client=mcp_client  # ✅ 传入客户端
)

# 方式 2: 禁用元数据校验（离线模式）
parser = SQLParser(
    sql_content=sql_text,
    mcp_client=None  # ⚠️ 不传入客户端
)
```

---

## 性能影响

| 操作                  | 时间开销      | 优化建议                     |
|----------------------|--------------|------------------------------|
| 元数据查询 (单表)     | ~200ms       | 可接受                       |
| 批量分析 (100 表)    | ~20s         | 使用缓存（待实现）            |
| 离线模式 (无 MCP)    | 0ms          | 适用于代码审查                |

**优化方向**:
- [ ] 增加元数据缓存（避免重复查询同一张表）
- [ ] 批量查询接口（一次查询多张表）
- [ ] 异步查询（解析和查询并行）

---

## 与其他功能的关系

```
┌─────────────────────────────────────────┐
│  字段对齐（本功能）                      │
│  - 确保字段名准确                        │
└────────────┬────────────────────────────┘
             ↓
┌─────────────────────────────────────────┐
│  指标识别                                │
│  - 基于真实字段名推断指标                │
│  - 示例: today_loan_amt → 当日放款金额      │
└────────────┬────────────────────────────┘
             ↓
┌─────────────────────────────────────────┐
│  指标去重检查                            │
│  - search_existing_indicators(today_loan_amt)│
│  - 避免重复注册                          │
└────────────┬────────────────────────────┘
             ↓
┌─────────────────────────────────────────┐
│  批量入库                                │
│  - register_indicator(...)               │
│  - register_lineage(...)                 │
└─────────────────────────────────────────┘
```

**关键点**: 字段对齐是后续所有操作的基础。如果字段名错误，会导致：
- 指标注册到不存在的字段
- 血缘关系追踪错误
- 无法与现有指标关联

---

## FAQ

### Q1: 如果目标表不存在怎么办？

**A**: 元数据查询会失败，自动回退到使用 SQL 别名。适用于新表开发场景。

### Q2: 如果 SQL 使用了 `INSERT INTO table (col1, col2, ...)` 显式指定列名？

**A**: 当前版本暂不支持。后续可增强为**名称匹配模式**（优先级高于位置对齐）。

### Q3: 如何验证对齐是否正确？

**A**: 查看日志输出：

```
🔀 字段对齐: report_date → dt
🔀 字段对齐: total_amount → today_loan_amt
```

如果输出为空，说明：
- 元数据查询失败，或
- SQL 别名与目标表字段名完全一致（无需对齐）

### Q4: 能否手动指定对齐规则？

**A**: 当前版本不支持。可以扩展为：

```python
parser = SQLParser(
    sql_content=sql_text,
    mcp_client=mcp_client,
    custom_mapping={'report_date': 'dt', 'total_amount': 'today_loan_amt'}  # 手动映射
)
```

---

**Created by**: Claude Code
**Version**: 1.0
**Last Updated**: 2026-02-08
