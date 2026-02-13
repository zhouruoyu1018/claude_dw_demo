# SQL 重构报告

## 基本信息

| 项目 | 内容 |
|------|------|
| 原脚本 | {source_sql_file} |
| 重构脚本 | {target_sql_file} |
| 重构范围 | {scope} |
| 引擎 | {source_engine}->{target_engine} |
| 执行模式 | {normal/dry-run} |
| 时间 | {YYYY-MM-DD HH:MM:SS} |

## 审查与重构摘要

### 原始问题统计（来自 review-sql）

| 级别 | 数量 |
|------|------|
| FATAL | {n} |
| ERROR | {n} |
| WARN | {n} |
| INFO | {n} |

### 本次应用规则

| 规则ID | 类型 | 风险 | 变更摘要 |
|--------|------|------|---------|
| {rule_id} | {compliance/code/perf/migrate} | {safe/low/medium/high} | {summary} |

## 变更明细

| 位置 | 修改前 | 修改后 | 规则ID |
|------|--------|--------|--------|
| L{line} | `{before}` | `{after}` | {rule_id} |

## 等价性校验

- [ ] 粒度一致（主键/Group By 不变）
- [ ] 字段语义一致（核心指标口径不变）
- [ ] 过滤条件一致（含分区过滤）
- [ ] JOIN 关系一致（无额外 N:N 风险）
- [ ] 迁移函数返回类型一致（仅 migrate）

### 校验结论

{pass_or_block_reason}

## 影响分析（Lineage）

| 下游对象 | 类型 | 影响等级 | 建议动作 |
|---------|------|---------|---------|
| {downstream_obj} | {table/job/report} | {low/medium/high} | {action} |

> 若 MCP 不可用，请记录：`影响分析跳过（MCP unavailable）`

## A/B 验证 SQL

### 1) 行数对比

```sql
{ab_sql_row_count}
```

### 2) 主键唯一性对比

```sql
{ab_sql_pk_unique}
```

### 3) 核心指标聚合对比

```sql
{ab_sql_metric_compare}
```

## 上线建议

1. 必须先通过 A/B 校验并记录结果
2. 若存在 `HIGH` 风险项，先灰度再全量替换
3. 若影响关键下游，先通知 owner 再发布
