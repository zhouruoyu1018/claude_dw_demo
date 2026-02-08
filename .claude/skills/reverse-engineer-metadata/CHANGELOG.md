# 更新日志 - Reverse Engineer Metadata

## [1.1.0] - 2026-02-08

### ✨ 新增功能

#### 字段对齐机制（Field Alignment）

**问题**: DML 中的 SELECT 别名不一定等于目标表的实际字段名

**解决方案**:
- 集成 MCP 元数据查询（`list_columns`）
- 实现位置对齐策略（Hive INSERT OVERWRITE 默认行为）
- 自动替换 SQL 别名为真实字段名

**示例**:

```sql
-- SQL 中的别名
SELECT
    report_date,      -- ← SQL 别名
    total_amount,
    cnt
FROM ...

-- 自动对齐为目标表真实字段
✅ report_date  → dt
✅ total_amount → td_loan_amt
✅ cnt          → td_loan_cnt
```

**影响范围**:
- 字段级血缘更准确
- 指标注册字段名正确
- 可与已有指标关联

**配置方式**:

```python
# 启用元数据校验（推荐）
parser = SQLParser(
    sql_content=sql_text,
    mcp_client=mcp_client  # ✨ 传入 MCP 客户端
)

# 禁用（离线模式）
parser = SQLParser(
    sql_content=sql_text,
    mcp_client=None
)
```

**文档**: 详见 `FIELD_ALIGNMENT.md`

---

### 🛡️ 容错机制

#### 1. 元数据查询失败

**行为**: 不中断解析流程，自动回退到使用 SQL 别名

```python
⚠️ 元数据查询失败: Connection timeout
ℹ️ 将使用 SQL 中的别名进行解析
```

#### 2. 字段数量不匹配

**场景**: SQL SELECT 字段数 ≠ 目标表字段数

**行为**: 发出警告，跳过对齐

```python
⚠️ 字段数量不匹配: SQL 8 vs 目标表 5
   SQL 字段: [report_date, total_amount, ...]
   目标表字段: [dt, td_loan_amt, ...]
ℹ️ 跳过字段对齐，使用 SQL 别名
```

**可能原因**:
- 目标表结构变更
- SQL 中有派生字段未持久化
- SQL 解析错误

#### 3. 离线模式

**场景**: 未传入 `mcp_client`

**行为**: 跳过元数据查询，直接使用 SQL 别名

**适用场景**:
- 新表开发（目标表不存在）
- 代码审查（无 MCP 连接）
- 快速原型验证

---

### 📝 日志增强

新增字段对齐日志输出：

```
🔀 字段对齐: report_date → dt
🔀 字段对齐: total_amount → td_loan_amt
🔀 字段对齐: cnt → td_loan_cnt
```

如果未输出，说明：
- 元数据查询失败，或
- SQL 别名与目标表字段名完全一致（无需对齐）

---

### 🧪 测试用例

已更新测试脚本 `test_parser.py`，所有测试通过：

```
✅ 测试 1: 基础 SQL 解析 - 通过
✅ 测试 2: 复杂 JOIN 解析 - 通过
✅ 测试 3: 窗口函数解析 - 通过
✅ 测试 4: CASE WHEN 解析 - 通过
✅ 测试 5: 解析示例 SQL 文件 - 通过
```

---

### 📚 文档更新

- ✅ 新增 `FIELD_ALIGNMENT.md` - 字段对齐机制详细说明
- ✅ 更新 `README.md` - 增加元数据校验说明
- ✅ 更新 `QUICKSTART.md` - 增加 MCP 客户端配置示例

---

### 🔧 代码变更

**核心文件**: `scripts/sql_parser.py`

**主要变更**:

1. `SQLParser.__init__` 新增 `mcp_client` 参数
2. 新增方法 `_fetch_target_columns_from_metadata()`
3. 新增方法 `_align_columns_with_metadata()`
4. 修改 `parse()` 流程，增加元数据查询和字段对齐步骤

**代码行数**: +60 行

**向后兼容**: ✅ 是（`mcp_client` 为可选参数）

---

### ⚠️ 已知限制

1. **仅支持位置对齐**: 不支持 `INSERT INTO table (col1, col2, ...)`（名称对齐）
2. **字段过滤规则硬编码**: 排除 `dt`, `create_time`, `update_time`（待配置化）
3. **无缓存机制**: 重复查询同一张表会多次调用 MCP（性能优化待实现）

---

### 🚀 后续计划

- [ ] 支持名称对齐模式（优先级高于位置对齐）
- [ ] 增加元数据缓存（避免重复查询）
- [ ] 支持手动指定字段映射规则
- [ ] 批量查询接口（一次查询多张表）
- [ ] 异步查询（解析和查询并行）

---

## [1.0.0] - 2026-02-08

### 🎉 初始版本

**核心功能**:
- ✅ SQL 解析（Hive/Impala/Doris）
- ✅ 表级血缘提取（FROM + JOIN）
- ✅ 字段级血缘提取（SELECT 到源字段映射）
- ✅ 指标自动识别（SUM/COUNT/AVG/MAX/MIN）
- ✅ 窗口函数识别（ROW_NUMBER/RANK/OVER）
- ✅ CASE WHEN 表达式识别
- ✅ 业务域、标准类型、更新频率智能推断
- ✅ 中文名生成（基于词根映射）
- ✅ 交互式确认和批量入库

**交付文件**:
- `SKILL.md` - Skill 定义
- `README.md` - 完整使用文档
- `QUICKSTART.md` - 快速开始指南
- `scripts/sql_parser.py` - SQL 解析引擎
- `scripts/test_parser.py` - 自动化测试套件
- `examples/sample_dm_loan_daily.sql` - 示例 SQL

**测试覆盖率**: 5 个测试用例，全部通过

---

**Created by**: Claude Code
**Maintainer**: Data Warehouse Team
