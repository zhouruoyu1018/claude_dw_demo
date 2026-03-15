# Step 8: 需求文件持久化

需求清单输出后，**自动写入文件**，确保跨会话不丢失需求上下文。

## 存储位置

```
docs/wip/req-{table_name}.md
```

- `{table_name}`: 从建议分层和需求主题推导的目标表名（如 `dmm_sac_loan_prod_daily`）
- 若 Phase 1 尚无法确定表名，使用需求主题的 snake_case 形式（如 `req-loan_product_daily_report.md`）
- 目录 `docs/wip/` 不存在时自动创建

## 文件内容

将完整的结构化需求清单（即 [output-template.md](output-template.md) 的内容）写入文件，并在文件头部追加元数据：

```markdown
---
status: wip
created: {YYYY-MM-DD}
source: dw-requirement-triage
target_table: {预估目标表名，待 Phase 3 确认}
engine: {建议引擎}
layer: {建议分层}
---

# 数仓需求清单

(... 完整的结构化需求清单内容 ...)
```

## 生命周期管理

| 阶段 | 文件状态 | 操作 |
|------|---------|------|
| Phase 1 完成 | `status: wip` | 创建文件 |
| Phase 3 完成 | `status: wip` | 更新 `target_table` 为确定的表名，文件重命名为 `req-{确定的表名}.md` |
| Phase 5 完成（全流程交付） | `status: done` | 更新状态；用户可选择移到 `docs/archive/` 或保留 |
| 用户主动清理 | 删除或归档 | `docs/archive/req-{table_name}.md` |

## 跨会话使用场景

当新会话通过 `--from=etl` 或 `--from=ddl` 进入工作流时：
1. 扫描 `docs/wip/` 目录，列出所有 `status: wip` 的需求文件
2. 询问用户选择对应的需求文件
3. 读取文件内容，恢复需求上下文
