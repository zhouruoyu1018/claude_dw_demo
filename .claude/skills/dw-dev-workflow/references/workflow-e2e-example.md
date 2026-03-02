# DW Workflow 端到端示例

## 用户输入

```text
需求: 运营需要一张放款产品日报表，统计每日各产品的放款金额、放款笔数，
     支持查看日环比变化。数据 T+1 更新即可。
```

## 期望执行轨迹（单表模式）

1. Phase 1 `dw-requirement-triage`  
识别需求类型、指标、维度、时间粒度、建议分层与引擎，等待用户确认。

2. Phase 2 `search-hive-metadata`  
优先查指标库复用，复用失败再查候选表与词根，输出“复用/新建”决策。

3. Phase 3 `generate-standard-ddl`  
按建模决策输出 DDL，显式给出粒度与主键。

4. Phase 4 `generate-etl-sql`  
生成 ETL 主体；执行指标入库确认与血缘注册（或降级为 MCP-PENDING 注释）。

5. Phase 5 `generate-qa-suite`  
生成冒烟测试 + DQC 规则 +（Doris 场景）性能分析请求。

## 典型交付物

- `{table_name}_ddl.sql`
- `{table_name}_etl.sql`
- `{table_name}_qa.sql`
- 指标库更新记录（若用户确认入库）
- 血缘库更新记录（若 MCP 可用）

## 多表模式补充

当 Phase 1 判定为多表需求时，先生成任务草案，再由 `dw-dev-workflow` 统一落盘 `plan-{project_name}.md` 并编排 DAG 执行。动态拆表请求由 `generate-etl-sql` 提交，编排细则仍以工作流为准。
