# 快速开始 - Reverse Engineer Metadata

## 5 分钟上手

### 步骤 1: 准备 SQL 文件

确保你有一个或多个 ETL SQL 脚本，格式如下：

```sql
INSERT OVERWRITE TABLE dm.dmm_loan_summary PARTITION (dt='...')
SELECT
    ...
FROM dwd.source_table
LEFT JOIN ...
```

### 步骤 2: 调用 Skill

```bash
# 方式 1: 分析单个文件
/reverse-engineer-metadata path/to/your/script.sql

# 方式 2: 批量分析目录
/reverse-engineer-metadata --scan sql/hive/etl/

# 方式 3: 试运行（仅生成报告）
/reverse-engineer-metadata script.sql --dry-run
```

### 步骤 3: 查看分析结果

Skill 会展示：

```
📊 分析结果

1️⃣ 表级血缘
   - 源表 A (FROM)
   - 源表 B (LEFT JOIN)
   ...

2️⃣ 字段级血缘
   - 目标字段 ← 源表.源字段 [转换类型]
   ...

3️⃣ 识别的指标
   - td_loan_amt (当日放款金额) - 金额 - 原子指标
   ...
```

### 步骤 4: 确认并入库

Claude 会询问：

```
✅ 请确认

1. 指标信息准确吗？
2. 血缘关系完整吗？
3. 确认后执行入库？

[✓ 确认入库] [✗ 取消] [✎ 修改指标]
```

选择 "确认入库" 后，数据会自动注册到元数据库。

---

## 实战示例

### 示例 1: 分析示例 SQL

```bash
/reverse-engineer-metadata .claude/skills/reverse-engineer-metadata/examples/sample_dm_loan_daily.sql
```

**预期输出**:
- 目标表: `dm.dmm_sac_loan_prod_daily`
- 源表: 3 张（dwd.dwd_loan_detail, dwd.dwd_product_info, dws.dws_overdue_summary）
- 识别指标: 8 个（td_loan_amt, td_loan_cnt, avg_loan_amt, success_rate 等）

### 示例 2: 批量分析 dm 层

```bash
/reverse-engineer-metadata --scan sql/hive/etl/ --filter "dm_*.sql"
```

**适用场景**: 项目交接时快速盘点现有资产

### 示例 3: 仅提取血缘（性能优化）

```bash
/reverse-engineer-metadata script.sql --skip-indicators
```

**适用场景**: 只关心数据流向，不需要指标信息

---

## 常见问题

### Q: 中文名推断不准怎么办？

**A**: 在确认环节选择 "修改指标"，手动输入正确的中文名和业务口径。

### Q: 如何避免重复入库？

**A**: Skill 内置去重检查，如果发现同名指标或血缘已存在，会提示你选择跳过或覆盖。

### Q: 支持哪些 SQL 引擎？

**A**:
- ✅ Hive (Tez)
- ✅ Impala
- ✅ Doris
- ⚠️ Spark SQL (部分支持)

### Q: 窗口函数能识别吗？

**A**: 能识别，会标记为 `CUSTOM` 类型并保留完整表达式。

---

## 下一步

- 📖 阅读完整文档: `README.md`
- 🧪 运行测试验证: `python scripts/test_parser.py`
- 🔧 配合其他 Skill: `/review-sql` 审查 SQL 规范性

---

**Created by**: Claude Code
**Last Updated**: 2026-02-08
