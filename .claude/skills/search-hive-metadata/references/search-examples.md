# 搜索示例和最佳实践

## 搜索场景

### 场景1: 已知表名关键词

**用户需求**: "查找订单相关的表"

**调用方式**:
```
search_table(keyword="order")
```

**返回示例**:
```
找到 8 个匹配的表:

| 表名 | 数据库 | 注释 |
|-----|-------|------|
| ods.ods_order_info | ods | 订单原始信息表 |
| dwd.dwd_order_detail | dwd | 订单明细宽表 |
| dws.dws_order_daily | dws | 订单日汇总表 |
| ads.ads_order_analysis | ads | 订单分析结果表 |
```

### 场景2: 按业务术语搜索

**用户需求**: "哪个表有放款金额字段"

**调用方式**:
```
search_by_comment(term="放款金额", search_scope="column")
```

**返回示例**:
```
找到字段匹配:

### dwd.dwd_loan_detail
- **匹配字段**: `loan_amount` (DECIMAL(18,2)): 放款金额

### ods.ods_loan_apply
- **匹配字段**: `apply_amount` (DECIMAL(18,2)): 申请放款金额
```

### 场景3: 限定分层搜索

**用户需求**: "查找 DWD 层的用户表"

**调用方式**:
```
search_table(keyword="user", schema_name="dwd")
```

### 场景4: 获取表详情

**用户需求**: "查看 dwd_order_detail 表的字段"

**调用方式**:
```
get_table_detail(table_name_full="dwd.dwd_order_detail")
```

**返回示例**:
```
## 表详情: dwd.dwd_order_detail

- **表注释**: 订单明细宽表，关联用户、商品、支付信息
- **分区键**: stat_date
- **数据量**: 1.2TB
- **负责人**: zhangsan

### 字段列表
| 字段名 | 类型 | 注释 |
|-------|-----|------|
| order_id | BIGINT | 订单ID |
| user_id | BIGINT | 用户ID |
| product_id | BIGINT | 商品ID |
| order_amount | DECIMAL(18,2) | 订单金额 |
| pay_amount | DECIMAL(18,2) | 实付金额 |
| order_time | TIMESTAMP | 下单时间 |
| pay_time | TIMESTAMP | 支付时间 |
| stat_date | STRING | 分区字段 |
```

### 场景5: 业务术语无法直接匹配元数据（智能搜索编排）

**用户需求**: "查找首逾金额相关的表"

**直接搜索无结果**:
```
search_by_comment(term="首逾金额") → 0 结果
```

**触发智能搜索编排 (S1-S6)**:

**S2 拆词**:
```
"首逾金额" → ["首逾", "金额"]
```

**S3 同义词扩展** (查 synonym.yaml):
```
"首逾" → ["M1逾期", "首次逾期", "首期逾期", "first_overdue", "首逾"]
"金额" → 区分度低，降低优先级
```

**S4 并行搜索** (选取高区分度候选):
```
search_by_comment("M1逾期")    → 2 张表
search_by_comment("首次逾期")  → 1 张表
search_by_comment("首逾")      → 0 结果（验证原词确实搜不到）
search_table("overdue")        → 1 张表
```

**S5 交叉排序**:
```
1. ph_sac_dwd.dwd_overdue_detail — 命中 "M1逾期" + "首次逾期"（2路命中）
2. ph_sac_dmm.dmm_sac_overdue_daily — 仅命中 "M1逾期"（1路命中，dm层加分）
3. ph_sac_dwd.dwd_loan_detail — 仅命中 search_table("overdue")
```

**S6 确认**: Top1 多路命中优势明显 → 自动选定 `dwd_overdue_detail`，展示匹配路径：
```
找到匹配（通过智能搜索编排）:
  搜索路径: "首逾金额" → 拆词["首逾"] → 同义词["M1逾期"] → 命中
  推荐表: ph_sac_dwd.dwd_overdue_detail
  匹配字段: first_overdue_amt (DECIMAL(18,2)): M1逾期金额
```

## 与 dw-requirement-triage 协作示例

### 完整工作流

**Step 1: 需求文档输入**

```
管理后台需增加"放款分析"模块，展示：
1. 每日放款金额和放款笔数
2. 各渠道放款占比
3. 放款金额 TOP10 客户
```

**Step 2: dw-requirement-triage 提取**

```markdown
### 需求1: 每日放款统计
- 指标: 放款金额, 放款笔数
- 维度: 日期
- 建议分层: ADS
```

**Step 3: search-hive-metadata 补全**

调用 `search_by_comment(term="放款金额")`:

```
找到匹配:
- dwd.dwd_loan_detail.loan_amount (放款金额)
- dwd.dwd_loan_detail.loan_id (可用于 COUNT)
- dwd.dwd_loan_detail.loan_date (放款日期)
```

调用 `search_by_comment(term="渠道")`:

```
找到匹配:
- dwd.dwd_loan_detail.channel_code (渠道编码)
- dim.dim_channel.channel_name (渠道名称)
```

**Step 4: 补全后的需求**

```markdown
### 需求1: 每日放款统计
- 指标: 放款金额 (SUM), 放款笔数 (COUNT)
- 维度: 日期
- 数据来源: dwd.dwd_loan_detail
- 相关字段:
  - loan_amount: 放款金额
  - loan_id: 放款ID (用于计数)
  - loan_date: 放款日期 (维度)
- 建议分层: ADS
- 建议引擎: Impala
```

## 最佳实践

### 1. 搜索策略

**优先级顺序**:
1. 先按表名精确搜索
2. 若无结果，按表名模糊搜索
3. 若仍无结果，按业务术语搜索

### 2. 缩小搜索范围

- 使用 `schema_name` 限定分层
- 使用 `search_scope` 限定搜索表注释或字段注释
- 使用 `limit` 控制返回数量

### 3. 处理多结果

当搜索返回多个结果时：
1. 根据分层优先级选择（ads > dws > dwd > ods）
2. 查看表注释判断业务含义
3. 如不确定，使用交互式提问让用户选择

### 4. 常见业务术语映射

贷款业务的完整同义词映射见 [synonym.yaml](synonym.yaml)，覆盖进件、授信、放款、贷后全生命周期。

以下为高频映射示例：

| 业务俗称 | 标准术语（元数据注释中的措辞） |
|---------|--------------------------|
| 进件 | 申请、授信申请 |
| 下款 | 放款、发放 |
| 首逾 | M1逾期、首次逾期 |
| 过件率 | 审批通过率、批核率 |
| 提款 | 支用、动支 |
| 结清 | 清偿、全部还款 |

### 5. 搜索无结果时的处理

当直接搜索无结果时，自动触发**智能搜索编排**流程（见 SKILL.md "智能搜索编排" 章节）：

1. 拆词: 将复合术语拆为最小语义单元
2. 同义词扩展: 查 synonym.yaml + Claude 领域知识
3. 并行多路召回: 高区分度术语优先，最多 4 次搜索
4. 交叉排序: 多路命中优先，dm/da 层优先
5. 若仍零命中: 展示所有已尝试的搜索词，建议用户提供更多线索
