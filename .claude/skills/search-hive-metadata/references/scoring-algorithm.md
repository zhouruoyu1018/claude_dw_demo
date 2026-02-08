# 多源消歧评分算法

## 综合评分函数

```python
def select_best_source(target_fields, query_grain, candidates):
    """
    多源消歧综合评分

    Args:
        target_fields: 需要查询的字段列表，如 ['loan_amt', 'loan_cnt']
        query_grain: 查询粒度（维度字段集合），如 {'product_code', 'dt'}
        candidates: 候选表列表（从 search_table/search_by_comment 返回）

    Returns:
        排序后的候选表列表，附带评分明细
    """
    scored = []

    for table in candidates:
        score = 0
        details = {}

        # === 1. 口径一致性检查 (40分 / 一票否决) ===
        indicator_match = search_existing_indicators(target_fields)
        if indicator_match and indicator_match['source_table'] == table['name']:
            score += 40
            details['caliber'] = f"+40 (指标库命中: {indicator_match['logic_desc']})"
        elif indicator_match and indicator_match['source_table'] != table['name']:
            details['caliber'] = "0 (指标库指定其他表)"
        else:
            details['caliber'] = "N/A (指标库未收录)"

        # === 2. 粒度匹配 (30分) ===
        table_grain = get_table_grain(table)  # 从 TBLPROPERTIES 或注释解析
        if table_grain == query_grain:
            score += 30
            details['grain'] = "+30 (粒度完全匹配)"
        elif query_grain.issubset(table_grain):
            score += 15
            details['grain'] = "+15 (表粒度更细，需聚合)"
        elif table_grain.issubset(query_grain):
            score += 0
            details['grain'] = "0 (表粒度更粗，无法使用)"
        else:
            score += 10
            details['grain'] = "+10 (粒度无法判断)"

        # === 3. 分层优先级 (20分) ===
        layer_scores = {
            'da_': 20, 'ads_': 20,
            'dm_': 18, 'dmm_': 18,
            'dws_': 15,
            'dim_': 12,
            'dwd_': 8,
            'ods_': 2,
            'tmp_': 0
        }
        for prefix, pts in layer_scores.items():
            if table['name'].startswith(prefix):
                score += pts
                details['layer'] = f"+{pts} (分层: {prefix})"
                break

        # === 4. 字段覆盖率 (10分) ===
        covered = len([f for f in target_fields if f in table['columns']])
        coverage_score = round((covered / len(target_fields)) * 10, 1)
        score += coverage_score
        details['coverage'] = f"+{coverage_score} (覆盖 {covered}/{len(target_fields)} 字段)"

        scored.append({
            'table': table['name'],
            'score': score,
            'details': details,
            'grain': table_grain
        })

    # 按分数降序排列
    scored.sort(key=lambda x: x['score'], reverse=True)
    return scored
```
