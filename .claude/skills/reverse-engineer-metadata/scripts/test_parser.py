#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL 解析器测试脚本
"""
import sys
import os
from pathlib import Path

# Windows 中文环境支持
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加当前目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent))

from sql_parser import SQLParser, parse_sql_file


def test_basic_parsing():
    """测试基础解析功能"""
    print("=" * 80)
    print("测试 1: 基础 SQL 解析")
    print("=" * 80)

    test_sql = """
    INSERT OVERWRITE TABLE ph_sac_dmm.dmm_sac_loan_prod_daily PARTITION (dt='${hivevar:dt}')
    SELECT
        a.dt,
        a.product_id,
        b.product_name,
        SUM(a.loan_amount) AS today_loan_amt,
        COUNT(DISTINCT a.loan_id) AS today_loan_cnt,
        SUM(CASE WHEN a.loan_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_cnt,
        SUM(a.loan_amount) / NULLIF(COUNT(DISTINCT a.loan_id), 0) AS avg_loan_amt
    FROM dwd.dwd_loan_detail a
    LEFT JOIN dwd.dwd_product_info b ON a.product_id = b.product_id
    WHERE a.dt = '${hivevar:dt}'
    GROUP BY a.dt, a.product_id, b.product_name
    """

    parser = SQLParser(test_sql, 'test.sql')
    result = parser.parse()

    print(f"\n✅ 目标表: {result.target_table}")
    assert result.target_table == 'ph_sac_dmm.dmm_sac_loan_prod_daily', "目标表解析失败"

    print(f"\n✅ 表级血缘 ({len(result.table_lineage)} 张源表):")
    for tl in result.table_lineage:
        print(f"   - {tl.source_table} ({tl.join_type})")
    assert len(result.table_lineage) == 2, "表级血缘数量不对"

    print(f"\n✅ 字段级血缘 ({len(result.column_lineage)} 个字段):")
    for cl in result.column_lineage[:5]:  # 只展示前 5 个
        source_info = f"{cl.source_table}.{cl.source_column}" if cl.source_table else cl.source_column
        print(f"   - {cl.target_column} ← {source_info} [{cl.transform_type}]")
    if len(result.column_lineage) > 5:
        print(f"   ... 共 {len(result.column_lineage)} 个字段")

    print(f"\n✅ 识别的指标 ({len(result.indicators)} 个):")
    for ind in result.indicators:
        print(f"   - {ind.indicator_english_name} ({ind.indicator_name}) - {ind.standard_type} - {ind.indicator_category}")

    print(f"\n✅ ETL 逻辑摘要: {result.etl_logic_summary}")
    print("\n✅ 测试 1 通过\n")


def test_complex_join():
    """测试复杂 JOIN"""
    print("=" * 80)
    print("测试 2: 复杂 JOIN 解析")
    print("=" * 80)

    test_sql = """
    INSERT INTO dm.dmm_loan_overdue_summary
    SELECT
        a.dt,
        a.loan_id,
        b.customer_id,
        c.overdue_days,
        d.collection_status
    FROM dwd.dwd_loan_detail a
    INNER JOIN dwd.dwd_customer_info b ON a.customer_id = b.customer_id
    LEFT JOIN dws.dws_overdue_summary c ON a.loan_id = c.loan_id AND a.dt = c.dt
    RIGHT JOIN dwd.dwd_collection_log d ON a.loan_id = d.loan_id
    WHERE a.dt = '2024-01-01'
    """

    parser = SQLParser(test_sql, 'complex_join.sql')
    result = parser.parse()

    print(f"\n✅ 目标表: {result.target_table}")

    print(f"\n✅ 表级血缘:")
    for tl in result.table_lineage:
        condition = f" ON {tl.join_condition}" if tl.join_condition else ""
        print(f"   - {tl.source_table} ({tl.join_type}){condition}")

    assert len(result.table_lineage) == 4, f"应该有 4 张源表，实际 {len(result.table_lineage)}"
    print("\n✅ 测试 2 通过\n")


def test_window_function():
    """测试窗口函数"""
    print("=" * 80)
    print("测试 3: 窗口函数解析")
    print("=" * 80)

    test_sql = """
    INSERT OVERWRITE TABLE dm.dmm_loan_rank
    SELECT
        a.dt,
        a.product_id,
        a.loan_amount,
        ROW_NUMBER() OVER (PARTITION BY a.product_id ORDER BY a.loan_amount DESC) AS loan_rank,
        SUM(a.loan_amount) OVER (PARTITION BY a.product_id) AS total_product_amt
    FROM dwd.dwd_loan_detail a
    WHERE a.dt = '2024-01-01'
    """

    parser = SQLParser(test_sql, 'window_func.sql')
    result = parser.parse()

    print(f"\n✅ 字段级血缘:")
    for cl in result.column_lineage:
        if cl.transform_type == 'CUSTOM':
            print(f"   - {cl.target_column} [{cl.transform_type}]")
            print(f"     表达式: {cl.transform_expr}")

    # 窗口函数应该被识别为 CUSTOM
    custom_fields = [cl for cl in result.column_lineage if cl.transform_type == 'CUSTOM']
    assert len(custom_fields) >= 1, "窗口函数未被识别"
    print("\n✅ 测试 3 通过\n")


def test_case_when():
    """测试 CASE WHEN 表达式"""
    print("=" * 80)
    print("测试 4: CASE WHEN 解析")
    print("=" * 80)

    test_sql = """
    INSERT INTO dm.dmm_loan_status
    SELECT
        dt,
        loan_id,
        CASE
            WHEN loan_status = 'SUCCESS' THEN '成功'
            WHEN loan_status = 'FAILED' THEN '失败'
            ELSE '未知'
        END AS status_desc,
        CASE WHEN overdue_days > 30 THEN 1 ELSE 0 END AS is_serious_overdue
    FROM dwd.dwd_loan_detail
    """

    parser = SQLParser(test_sql, 'case_when.sql')
    result = parser.parse()

    print(f"\n✅ 字段级血缘:")
    for cl in result.column_lineage:
        if cl.transform_type == 'CASE':
            print(f"   - {cl.target_column} [CASE]")

    case_fields = [cl for cl in result.column_lineage if cl.transform_type == 'CASE']
    assert len(case_fields) >= 1, "CASE WHEN 未被识别"
    print("\n✅ 测试 4 通过\n")


def test_sample_file():
    """测试示例 SQL 文件"""
    print("=" * 80)
    print("测试 5: 解析示例 SQL 文件")
    print("=" * 80)

    sample_file = Path(__file__).parent.parent / 'examples' / 'sample_dm_loan_daily.sql'
    if not sample_file.exists():
        print(f"⚠️ 示例文件不存在: {sample_file}")
        return

    result = parse_sql_file(str(sample_file))

    print(f"\n✅ 目标表: {result.target_table}")
    print(f"\n✅ 源表数量: {len(result.table_lineage)}")
    print(f"\n✅ 字段数量: {len(result.column_lineage)}")
    print(f"\n✅ 指标数量: {len(result.indicators)}")

    print(f"\n✅ 识别的指标:")
    for ind in result.indicators:
        print(f"   - {ind.indicator_english_name:20s} | {ind.indicator_name:15s} | {ind.standard_type:6s} | {ind.indicator_category}")

    print("\n✅ 测试 5 通过\n")


def run_all_tests():
    """运行所有测试"""
    try:
        test_basic_parsing()
        test_complex_join()
        test_window_function()
        test_case_when()
        test_sample_file()

        print("=" * 80)
        print("🎉 所有测试通过！")
        print("=" * 80)
        return 0

    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ 执行错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(run_all_tests())
