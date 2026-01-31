#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
血缘管理功能测试脚本

1. 创建 data_lineage 和 column_lineage 表
2. 插入测试数据
3. 测试 MCP 工具功能
"""

import os
import sys
import io

# 设置 stdout 编码为 utf-8 (Windows 兼容)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加 scripts 目录到 path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2
from psycopg2.extras import RealDictCursor

def get_pg_connection():
    """获取 PostgreSQL 连接"""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER", "root"),
        password=os.getenv("PG_PASSWORD", "123456"),
        dbname=os.getenv("PG_DATABASE", "phslm"),
        connect_timeout=10
    )


def create_tables():
    """创建血缘表"""
    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        # 删除旧表
        cursor.execute("DROP TABLE IF EXISTS column_lineage CASCADE")
        cursor.execute("DROP TABLE IF EXISTS data_lineage CASCADE")

        # 创建 data_lineage 表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                id SERIAL PRIMARY KEY,
                target_table VARCHAR(200) NOT NULL,
                target_schema VARCHAR(100),
                source_table VARCHAR(200) NOT NULL,
                source_schema VARCHAR(100),
                relation_type VARCHAR(50) DEFAULT 'ETL',
                join_type VARCHAR(50),
                etl_script_path VARCHAR(500),
                etl_logic_summary VARCHAR(1000),
                created_by VARCHAR(100) DEFAULT 'auto',
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)

        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lineage_target ON data_lineage(target_table)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lineage_source ON data_lineage(source_table)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lineage_active ON data_lineage(is_active)")

        # 创建 column_lineage 表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS column_lineage (
                id SERIAL PRIMARY KEY,
                target_table VARCHAR(200) NOT NULL,
                target_column VARCHAR(200) NOT NULL,
                source_table VARCHAR(200) NOT NULL,
                source_column VARCHAR(200) NOT NULL,
                transform_type VARCHAR(50),
                transform_expr VARCHAR(1000),
                table_lineage_id INT REFERENCES data_lineage(id) ON DELETE SET NULL,
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_col_lineage_target ON column_lineage(target_table, target_column)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_col_lineage_source ON column_lineage(source_table, source_column)")

        conn.commit()
        print("[OK] 表创建成功")

    finally:
        cursor.close()
        conn.close()


def insert_test_data():
    """插入测试数据"""
    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        # 清空现有数据
        cursor.execute("DELETE FROM column_lineage")
        cursor.execute("DELETE FROM data_lineage")

        # 插入表级血缘数据
        table_lineage_data = [
            # dm.dmm_sac_loan_prod_daily
            ('dm.dmm_sac_loan_prod_daily', 'dm', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM',
             'sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql', '按产品维度聚合当日放款明细', 'test_user'),
            ('dm.dmm_sac_loan_prod_daily', 'dm', 'dim.dim_product', 'dim', 'ETL', 'LEFT JOIN',
             'sql/hive/etl/dm/dmm_sac_loan_prod_daily_etl.sql', '关联产品维度获取产品名称', 'test_user'),

            # dm.dmm_sac_loan_chn_daily
            ('dm.dmm_sac_loan_chn_daily', 'dm', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM',
             'sql/hive/etl/dm/dmm_sac_loan_chn_daily_etl.sql', '按渠道维度聚合当日放款明细', 'test_user'),
            ('dm.dmm_sac_loan_chn_daily', 'dm', 'dim.dim_channel', 'dim', 'ETL', 'LEFT JOIN',
             'sql/hive/etl/dm/dmm_sac_loan_chn_daily_etl.sql', '关联渠道维度获取渠道名称', 'test_user'),

            # da.da_loan_report
            ('da.da_loan_report', 'da', 'dm.dmm_sac_loan_prod_daily', 'dm', 'ETL', 'FROM',
             'sql/hive/etl/da/da_loan_report_etl.sql', '汇总产品维度指标到报表层', 'test_user'),
            ('da.da_loan_report', 'da', 'dm.dmm_sac_loan_chn_daily', 'dm', 'ETL', 'LEFT JOIN',
             'sql/hive/etl/da/da_loan_report_etl.sql', '关联渠道维度指标', 'test_user'),

            # dwd.dwd_loan_detail
            ('dwd.dwd_loan_detail', 'dwd', 'ods.ods_loan_apply', 'ods', 'ETL', 'FROM',
             'sql/hive/etl/dwd/dwd_loan_detail_etl.sql', '清洗贷款申请数据', 'test_user'),
            ('dwd.dwd_loan_detail', 'dwd', 'ods.ods_loan_contract', 'ods', 'ETL', 'LEFT JOIN',
             'sql/hive/etl/dwd/dwd_loan_detail_etl.sql', '关联合同信息补充放款字段', 'test_user'),

            # dws.dws_cust_loan_summary
            ('dws.dws_cust_loan_summary', 'dws', 'dwd.dwd_loan_detail', 'dwd', 'ETL', 'FROM',
             'sql/hive/etl/dws/dws_cust_loan_summary_etl.sql', '按客户维度汇总贷款信息', 'test_user'),
        ]

        cursor.executemany("""
            INSERT INTO data_lineage
            (target_table, target_schema, source_table, source_schema, relation_type, join_type,
             etl_script_path, etl_logic_summary, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, table_lineage_data)

        print(f"[OK] 插入 {len(table_lineage_data)} 条表级血缘数据")

        # 插入字段级血缘数据
        column_lineage_data = [
            # dm.dmm_sac_loan_prod_daily
            ('dm.dmm_sac_loan_prod_daily', 'product_code', 'dwd.dwd_loan_detail', 'product_code', 'DIRECT', 'product_code', 1),
            ('dm.dmm_sac_loan_prod_daily', 'product_name', 'dim.dim_product', 'product_name', 'DIRECT', 'product_name', 2),
            ('dm.dmm_sac_loan_prod_daily', 'td_sum_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'SUM', 'SUM(loan_amount)', 1),
            ('dm.dmm_sac_loan_prod_daily', 'td_cnt_loan', 'dwd.dwd_loan_detail', 'loan_id', 'COUNT', 'COUNT(loan_id)', 1),
            ('dm.dmm_sac_loan_prod_daily', 'td_diff_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'CUSTOM', 'SUM(loan_amount) - LAG(SUM(loan_amount))', 1),

            # dm.dmm_sac_loan_chn_daily
            ('dm.dmm_sac_loan_chn_daily', 'channel_code', 'dwd.dwd_loan_detail', 'channel_code', 'DIRECT', 'channel_code', 3),
            ('dm.dmm_sac_loan_chn_daily', 'channel_name', 'dim.dim_channel', 'channel_name', 'DIRECT', 'channel_name', 4),
            ('dm.dmm_sac_loan_chn_daily', 'td_sum_loan_amt', 'dwd.dwd_loan_detail', 'loan_amount', 'SUM', 'SUM(loan_amount)', 3),
            ('dm.dmm_sac_loan_chn_daily', 'td_cnt_loan', 'dwd.dwd_loan_detail', 'loan_id', 'COUNT', 'COUNT(loan_id)', 3),

            # da.da_loan_report
            ('da.da_loan_report', 'total_loan_amt', 'dm.dmm_sac_loan_prod_daily', 'td_sum_loan_amt', 'SUM', 'SUM(td_sum_loan_amt)', 5),
            ('da.da_loan_report', 'total_loan_cnt', 'dm.dmm_sac_loan_prod_daily', 'td_cnt_loan', 'SUM', 'SUM(td_cnt_loan)', 5),
        ]

        cursor.executemany("""
            INSERT INTO column_lineage
            (target_table, target_column, source_table, source_column, transform_type, transform_expr, table_lineage_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, column_lineage_data)

        print(f"[OK] 插入 {len(column_lineage_data)} 条字段级血缘数据")

        conn.commit()

    finally:
        cursor.close()
        conn.close()


def verify_data():
    """验证数据"""
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 查看表级血缘
        print("\n" + "="*60)
        print("表级血缘数据")
        print("="*60)
        cursor.execute("SELECT id, target_table, source_table, join_type, etl_logic_summary FROM data_lineage ORDER BY id")
        for row in cursor.fetchall():
            print(f"  [{row['id']}] {row['target_table']} ← {row['source_table']} ({row['join_type']})")

        # 查看字段级血缘
        print("\n" + "="*60)
        print("字段级血缘数据")
        print("="*60)
        cursor.execute("SELECT id, target_table, target_column, source_table, source_column, transform_type FROM column_lineage ORDER BY id")
        for row in cursor.fetchall():
            print(f"  [{row['id']}] {row['target_table']}.{row['target_column']} ← {row['source_table']}.{row['source_column']} ({row['transform_type']})")

    finally:
        cursor.close()
        conn.close()


def test_mcp_functions():
    """测试 MCP 函数"""
    # 导入 mcp_server 中的函数
    from mcp_server import (
        register_lineage,
        search_lineage_upstream,
        search_lineage_downstream
    )

    print("\n" + "="*60)
    print("测试 MCP 函数")
    print("="*60)

    # 测试 1: 查询上游依赖
    print("\n--- 测试 search_lineage_upstream ---")
    print("查询 dm.dmm_sac_loan_prod_daily 的上游依赖:")
    result = search_lineage_upstream("dm.dmm_sac_loan_prod_daily", depth=1, include_columns=True)
    print(f"  上游表数量: {result['total_upstream']}")
    for up in result['upstream_tables']:
        print(f"  • {up['source']} ({up['join_type']}) - {up['logic_summary']}")
    if result.get('column_lineage'):
        print(f"  字段血缘:")
        for cl in result['column_lineage'][:3]:  # 只显示前3条
            print(f"    • {cl['target_column']} ← {cl['source_table']}.{cl['source_column']} ({cl['transform_type']})")

    # 测试 2: 查询下游影响
    print("\n--- 测试 search_lineage_downstream ---")
    print("查询 dwd.dwd_loan_detail 的下游影响:")
    result = search_lineage_downstream("dwd.dwd_loan_detail", depth=1)
    print(f"  下游表数量: {result['total_downstream']}")
    for down in result['downstream_tables']:
        print(f"  • {down['target']} ({down['join_type']}) - {down['logic_summary']}")

    # 测试 3: 深度查询（2层）
    print("\n--- 测试 search_lineage_downstream (depth=2) ---")
    print("查询 dwd.dwd_loan_detail 的下游影响 (2层):")
    result = search_lineage_downstream("dwd.dwd_loan_detail", depth=2)
    print(f"  下游表数量: {result['total_downstream']}")
    for down in result['downstream_tables']:
        print(f"  • [深度{down['depth']}] {down['target']} ({down['join_type']})")

    # 测试 4: 注册新血缘
    print("\n--- 测试 register_lineage ---")
    print("注册新血缘: test.test_target ← dwd.dwd_loan_detail")
    result = register_lineage(
        target_table="test.test_target",
        source_tables=[
            {"source_table": "dwd.dwd_loan_detail", "join_type": "FROM"},
            {"source_table": "dim.dim_product", "join_type": "LEFT JOIN"}
        ],
        etl_logic_summary="测试血缘注册",
        column_lineage=[
            {
                "target_column": "loan_amt",
                "source_table": "dwd.dwd_loan_detail",
                "source_column": "loan_amount",
                "transform_type": "SUM",
                "transform_expr": "SUM(loan_amount)"
            }
        ],
        created_by="test_script"
    )
    print(f"  表级血缘: {result['summary']['table_lineage_count']} 条")
    print(f"  字段级血缘: {result['summary']['column_lineage_count']} 条")
    for tl in result['table_lineage']:
        print(f"    • [{tl['action']}] {result['target_table']} ← {tl['source_table']}")

    print("\n[OK] MCP 函数测试完成")


def main():
    print("="*60)
    print("血缘管理功能测试")
    print("="*60)

    try:
        # Step 1: 创建表
        print("\n[Step 1] 创建表...")
        create_tables()

        # Step 2: 插入测试数据
        print("\n[Step 2] 插入测试数据...")
        insert_test_data()

        # Step 3: 验证数据
        print("\n[Step 3] 验证数据...")
        verify_data()

        # Step 4: 测试 MCP 函数
        print("\n[Step 4] 测试 MCP 函数...")
        test_mcp_functions()

        print("\n" + "="*60)
        print("全部测试完成!")
        print("="*60)

    except Exception as e:
        print(f"\n[ERROR] 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
