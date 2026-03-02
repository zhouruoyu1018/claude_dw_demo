# 交互与协作指南

## 交互式确认模板

以下场景应主动询问用户：

1. 映射歧义：目标字段可对应多个源字段
2. 粒度不匹配：需聚合或需下钻
3. 增量 vs 全量：调度策略不明确
4. JOIN 类型：LEFT/INNER 语义影响结果范围
5. NULL 处理：0 与 NULL 的业务语义差异
6. 指标入库：候选新指标是否入库
7. JOIN 键近名歧义：多个相似字段可能误选
8. JOIN 键语义不匹配：编码/ID/PSID 类型不一致

推荐问法应包含：

- 当前判断
- 风险点
- 2-4 个可选答案
- 默认推荐项（如存在）

## 协作链路

```text
需求文档
  -> dw-requirement-triage
  -> search-hive-metadata
  -> generate-standard-ddl
  -> generate-etl-sql
  -> 调度上线 / QA
```

## 前置依赖契约

| Skill | 依赖输入 |
|-------|---------|
| `dw-requirement-triage` | 需求字段列表、引擎建议 |
| `search-hive-metadata` | 源表 Schema、指标复用信息、词根 |
| `generate-standard-ddl` | 目标表 DDL（主键、分区、COMMENT） |

## 失败与降级协作

- MCP 查询不可用：请求用户补充关键元数据，不阻塞 SQL 生成
- MCP 写入不可用：输出 `MCP-PENDING` 注释，后补录
- 编排冲突：以 `dw-dev-workflow` 为编排真源
