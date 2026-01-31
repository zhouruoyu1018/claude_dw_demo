#!/usr/bin/env python3
"""
Hive Metadata Search MCP Server

连接 MySQL 元数据库和 PostgreSQL 数据库，提供 Hive 表元数据搜索服务。
- MySQL: 存储 tbl_base_info（Hive 表元数据）
- PostgreSQL: 存储 indicator_registry（指标库）和 word_root_dict（词根字典）

支持表名搜索、业务术语搜索、字段查询、指标复用检索、词根查询等功能。

Usage:
    python mcp_server.py

Environment Variables:
    MySQL (用于 tbl_base_info):
        MYSQL_HOST: MySQL 主机地址 (默认 localhost)
        MYSQL_PORT: MySQL 端口 (默认 3306)
        MYSQL_USER: MySQL 用户名 (默认 root)
        MYSQL_PASSWORD: MySQL 密码
        MYSQL_DATABASE: 元数据库名称 (默认 hive_metadata)

    PostgreSQL (用于 indicator_registry 和 word_root_dict):
        PG_HOST: PostgreSQL 主机地址 (默认 localhost)
        PG_PORT: PostgreSQL 端口 (默认 5432)
        PG_USER: PostgreSQL 用户名 (默认 postgres)
        PG_PASSWORD: PostgreSQL 密码
        PG_DATABASE: 数据库名称 (默认 indicator_db)
"""

import json
import os
import sys
from typing import Any

# MCP SDK imports
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
except ImportError:
    print("请先安装 MCP SDK: pip install mcp", file=sys.stderr)
    sys.exit(1)

# MySQL connector (使用 pymysql)
try:
    import pymysql
    from pymysql import Error as MySQLError
    from pymysql.cursors import DictCursor
except ImportError:
    print("请先安装 PyMySQL: pip install pymysql", file=sys.stderr)
    sys.exit(1)

# PostgreSQL connector (使用 psycopg2)
try:
    import psycopg2
    from psycopg2 import Error as PGError
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("请先安装 psycopg2: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


# ============== 数据库配置 ==============

def get_db_config() -> dict:
    """从环境变量获取数据库配置"""
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", "hive_metadata"),
        "charset": "utf8mb4",
        "connect_timeout": 10
    }


def get_connection():
    """获取 MySQL 数据库连接（用于 tbl_base_info 表）"""
    config = get_db_config()
    return pymysql.connect(**config)


def get_pg_config() -> dict:
    """从环境变量获取 PostgreSQL 数据库配置（用于 indicator_registry、word_root_dict、data_lineage）"""
    return {
        "host": os.getenv("PG_HOST", "127.0.0.1"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "user": os.getenv("PG_USER", "root"),
        "password": os.getenv("PG_PASSWORD", "123456"),
        "dbname": os.getenv("PG_DATABASE", "phslm"),
        "connect_timeout": 10
    }


def get_pg_connection():
    """获取 PostgreSQL 数据库连接（用于 indicator_registry 和 word_root_dict）"""
    config = get_pg_config()
    return psycopg2.connect(**config)


# ============== 搜索功能 ==============

def search_table(keyword: str, schema_name: str = None, limit: int = 10) -> list[dict]:
    """
    按表名搜索 Hive 表

    Args:
        keyword: 表名关键词，支持模糊匹配
        schema_name: 限定数据库名（可选）
        limit: 返回结果数量

    Returns:
        匹配的表列表
    """
    conn = get_connection()
    cursor = conn.cursor(DictCursor)

    try:
        sql = """
            SELECT
                table_name_full,
                table_name,
                schema_name,
                table_comment,
                total_data_size_display,
                tbl_row_cnt,
                column_cnt,
                partition_key,
                tbl_strg_format
            FROM tbl_base_info
            WHERE table_name_full LIKE %s
        """
        params = [f"%{keyword}%"]

        if schema_name:
            sql += " AND schema_name = %s"
            params.append(schema_name)

        sql += " ORDER BY tbl_row_cnt DESC LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        results = cursor.fetchall()

        return results

    finally:
        cursor.close()
        conn.close()


def parse_column_list(column_list_str: str) -> list[dict]:
    """
    解析 column_list JSON 字符串，兼容新旧格式，带容错处理

    新格式: {"columns": [{"id": "...", "column_name": "...", "type_name": "...", "comment_name": "..."}]}
    旧格式: [{"name": "...", "type": "...", "comment": "..."}]

    Returns:
        统一格式的字段列表: [{"name": "...", "type": "...", "comment": "..."}]
    """
    if not column_list_str:
        return []

    def normalize_columns(data) -> list[dict]:
        """将解析后的数据标准化为统一格式"""
        # 新格式: {"columns": [...]}
        if isinstance(data, dict) and "columns" in data:
            columns = data["columns"]
        elif isinstance(data, list):
            columns = data
        else:
            return []

        return [
            {
                "name": col.get("column_name") or col.get("name", ""),
                "type": col.get("type_name") or col.get("type", ""),
                "comment": col.get("comment_name") or col.get("comment", "")
            }
            for col in columns
            if isinstance(col, dict)
        ]

    # 尝试 1: 直接解析
    try:
        data = json.loads(column_list_str)
        return normalize_columns(data)
    except json.JSONDecodeError:
        pass

    # 尝试 2: 修复常见的 JSON 格式问题（对象之间缺少逗号）
    try:
        import re
        # 修复 }{ 变成 },{
        fixed = re.sub(r'\}\s*\{', '},{', column_list_str)
        data = json.loads(fixed)
        return normalize_columns(data)
    except json.JSONDecodeError:
        pass

    # 尝试 3: 使用正则表达式提取字段信息
    try:
        import re
        columns = []
        # 匹配 column_name 和 type_name
        pattern = r'"column_name"\s*:\s*"([^"]*)".*?"type_name"\s*:\s*"([^"]*)".*?"comment_name"\s*:\s*"([^"]*)"'
        for match in re.finditer(pattern, column_list_str):
            columns.append({
                "name": match.group(1),
                "type": match.group(2),
                "comment": match.group(3)
            })
        if columns:
            return columns
    except Exception:
        pass

    return []


def search_by_comment(term: str, search_scope: str = "all", limit: int = 10) -> list[dict]:
    """
    按业务术语（注释）搜索

    Args:
        term: 业务术语，如"申请时间"、"放款金额"
        search_scope: 搜索范围 - "table"(表注释) | "column"(字段注释) | "all"(全部)
        limit: 返回结果数量

    Returns:
        匹配的表和字段列表
    """
    conn = get_connection()
    cursor = conn.cursor(DictCursor)

    try:
        results = []

        # 搜索表注释
        if search_scope in ("table", "all"):
            sql_table = """
                SELECT
                    table_name_full,
                    table_name,
                    schema_name,
                    table_comment,
                    'table' as match_type,
                    NULL as matched_column
                FROM tbl_base_info
                WHERE table_comment LIKE %s
                ORDER BY tbl_row_cnt DESC
                LIMIT %s
            """
            cursor.execute(sql_table, [f"%{term}%", limit])
            results.extend(cursor.fetchall())

        # 搜索字段注释 (column_list 是 JSON 格式)
        if search_scope in ("column", "all"):
            sql_column = """
                SELECT
                    table_name_full,
                    table_name,
                    schema_name,
                    table_comment,
                    'column' as match_type,
                    column_list
                FROM tbl_base_info
                WHERE column_list LIKE %s
                ORDER BY tbl_row_cnt DESC
                LIMIT %s
            """
            cursor.execute(sql_column, [f"%{term}%", limit])
            column_results = cursor.fetchall()

            # 解析 column_list 找到匹配的字段
            for row in column_results:
                matched_columns = []
                column_list = row.get("column_list", "")

                if column_list:
                    columns = parse_column_list(column_list)
                    for col in columns:
                        col_comment = col.get("comment", "") or ""
                        col_name = col.get("name", "") or ""
                        if term in col_comment or term in col_name:
                            matched_columns.append(col)

                    # 如果解析失败但文本匹配，保留原始信息
                    if not columns and term in column_list:
                        matched_columns.append({"raw": column_list[:200]})

                if matched_columns:
                    row["matched_columns"] = matched_columns
                    row.pop("column_list", None)
                    results.append(row)

        return results[:limit]

    finally:
        cursor.close()
        conn.close()


def get_table_detail(table_name_full: str) -> dict | None:
    """
    获取表的详细信息

    Args:
        table_name_full: 完整表名，如 "ods.ods_order_info"

    Returns:
        表详情字典
    """
    conn = get_connection()
    cursor = conn.cursor(DictCursor)

    try:
        sql = """
            SELECT *
            FROM tbl_base_info
            WHERE table_name_full = %s
        """
        cursor.execute(sql, [table_name_full])
        result = cursor.fetchone()

        if result:
            # 解析 column_list
            if result.get("column_list"):
                result["columns"] = parse_column_list(result["column_list"])
                result.pop("column_list", None)

            # 解析 partition_list
            if result.get("partition_list"):
                try:
                    partition_data = json.loads(result["partition_list"])
                    if isinstance(partition_data, dict) and "partitions" in partition_data:
                        result["partitions"] = partition_data["partitions"]
                    elif isinstance(partition_data, list):
                        result["partitions"] = partition_data
                except json.JSONDecodeError:
                    result["partitions_raw"] = result["partition_list"][:500]
                result.pop("partition_list", None)

        return result

    finally:
        cursor.close()
        conn.close()


def list_columns(table_name_full: str) -> list[dict]:
    """
    获取表的字段列表

    Args:
        table_name_full: 完整表名

    Returns:
        字段列表，格式: [{"name": "...", "type": "...", "comment": "..."}]
    """
    conn = get_connection()
    cursor = conn.cursor(DictCursor)

    try:
        sql = """
            SELECT column_list
            FROM tbl_base_info
            WHERE table_name_full = %s
        """
        cursor.execute(sql, [table_name_full])
        result = cursor.fetchone()

        if not result or not result.get("column_list"):
            return []

        return parse_column_list(result["column_list"])

    finally:
        cursor.close()
        conn.close()


def search_word_root(keyword: str, tag: str = None, limit: int = 20) -> list[dict]:
    """
    搜索词根表，获取标准词根用于字段命名（从 PostgreSQL 查询）

    Args:
        keyword: 搜索关键词，支持中文业务含义或英文词根
        tag: 词根分类标签筛选（可选），如 'BIZ_ENTITY', 'CATEGORY_WORD'
        limit: 返回结果数量

    Returns:
        匹配的词根列表，每条包含:
        - english_abbr: 英文缩写（用于字段命名）
        - chinese_name: 中文名称
        - english_name: 英文全称
        - alias: 别名
        - tag: 分类标签
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        sql = """
            SELECT
                english_abbr,
                chinese_name,
                english_name,
                alias,
                tag
            FROM word_root_dict
            WHERE (chinese_name LIKE %s
               OR alias LIKE %s
               OR english_name LIKE %s
               OR english_abbr LIKE %s)
        """
        kw = f"%{keyword}%"
        params = [kw, kw, kw, kw]

        if tag:
            sql += " AND tag = %s"
            params.append(tag)

        sql += " ORDER BY english_abbr LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    finally:
        cursor.close()
        conn.close()


def search_existing_indicators(metric_name: str, limit: int = 10) -> list[dict]:
    """
    搜索已存在的指标，优先复用 DWS/ADS 层已计算好的指标（从 PostgreSQL 查询）

    Args:
        metric_name: 业务指标名称，如 '复购率', 'GMV', '日销售额'
        limit: 返回结果数量

    Returns:
        匹配的指标列表，包含:
        - indicator_name: 指标名称
        - indicator_code: 指标编码
        - indicator_english_name: 英文名/物理字段名
        - indicator_category: 指标分类
        - business_domain: 业务域
        - data_source: 数据来源
        - statistical_caliber: 业务口径
        - calculation_logic: 取值逻辑
        - it_owner: IT负责人
        - status: 状态
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        sql = """
            SELECT
                indicator_name,
                indicator_code,
                indicator_alias,
                indicator_english_name,
                indicator_category,
                business_domain,
                data_source,
                statistical_caliber,
                calculation_logic,
                data_type,
                it_owner,
                business_owner,
                create_time,
                update_time,
                status
            FROM indicator_registry
            WHERE indicator_name LIKE %s
               OR indicator_alias LIKE %s
               OR indicator_english_name LIKE %s
               OR statistical_caliber LIKE %s
            ORDER BY update_time DESC
            LIMIT %s
        """
        keyword = f"%{metric_name}%"
        cursor.execute(sql, [keyword, keyword, keyword, keyword, limit])
        results = [dict(row) for row in cursor.fetchall()]

        # 转换 datetime 为字符串，并标记匹配类型
        for row in results:
            for key in ("create_time", "update_time"):
                if row.get(key):
                    row[key] = str(row[key])

            # 判断匹配精确度
            if row.get("indicator_name") == metric_name:
                row["match_type"] = "perfect"
            elif metric_name in (row.get("indicator_name") or ""):
                row["match_type"] = "high"
            elif metric_name in (row.get("indicator_alias") or ""):
                row["match_type"] = "high"
            else:
                row["match_type"] = "partial"

        return results

    finally:
        cursor.close()
        conn.close()


# ============== 血缘管理功能 ==============

def register_lineage(
    target_table: str,
    source_tables: list[dict],
    etl_script_path: str = None,
    etl_logic_summary: str = None,
    column_lineage: list[dict] = None,
    created_by: str = "auto"
) -> dict:
    """
    注册表级和字段级血缘关系（写入 PostgreSQL）

    Args:
        target_table: 目标表完整名，如 'dm.dmm_sac_loan_prod_daily'
        source_tables: 源表列表，每条包含:
            - source_table (str): 源表完整名
            - join_type (str): JOIN 类型，如 'FROM', 'LEFT JOIN', 'INNER JOIN'
            - relation_type (str): 关系类型，默认 'ETL'
        etl_script_path: ETL 脚本路径（可选）
        etl_logic_summary: ETL 逻辑摘要（可选）
        column_lineage: 字段级血缘列表（可选），每条包含:
            - target_column (str): 目标字段名
            - source_table (str): 源表完整名
            - source_column (str): 源字段名
            - transform_type (str): 转换类型，如 'DIRECT', 'SUM', 'COUNT', 'CASE'
            - transform_expr (str): 转换表达式
        created_by: 创建人标识

    Returns:
        注册结果摘要
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 解析目标表的 schema
        target_schema = target_table.split('.')[0] if '.' in target_table else None

        registered_table_lineage = []
        registered_column_lineage = []

        # 1. 注册表级血缘
        for src in source_tables:
            source_table = src.get("source_table", "").strip()
            if not source_table:
                continue

            source_schema = source_table.split('.')[0] if '.' in source_table else None
            join_type = src.get("join_type", "FROM").strip()
            relation_type = src.get("relation_type", "ETL").strip()

            # 检查是否已存在相同的血缘记录
            cursor.execute(
                """SELECT id FROM data_lineage
                   WHERE target_table = %s AND source_table = %s AND is_active = TRUE""",
                [target_table, source_table]
            )
            existing = cursor.fetchone()

            if existing:
                # 更新现有记录
                cursor.execute(
                    """UPDATE data_lineage
                       SET join_type = %s, relation_type = %s,
                           etl_script_path = COALESCE(%s, etl_script_path),
                           etl_logic_summary = COALESCE(%s, etl_logic_summary),
                           updated_time = NOW()
                       WHERE id = %s""",
                    [join_type, relation_type, etl_script_path, etl_logic_summary, existing['id']]
                )
                registered_table_lineage.append({
                    "action": "updated",
                    "target_table": target_table,
                    "source_table": source_table,
                    "join_type": join_type
                })
            else:
                # 插入新记录
                cursor.execute(
                    """INSERT INTO data_lineage
                       (target_table, target_schema, source_table, source_schema,
                        relation_type, join_type, etl_script_path, etl_logic_summary,
                        created_by, created_time, updated_time, is_active)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), TRUE)
                       RETURNING id""",
                    [target_table, target_schema, source_table, source_schema,
                     relation_type, join_type, etl_script_path, etl_logic_summary, created_by]
                )
                new_id = cursor.fetchone()['id']
                registered_table_lineage.append({
                    "action": "inserted",
                    "id": new_id,
                    "target_table": target_table,
                    "source_table": source_table,
                    "join_type": join_type
                })

        # 2. 注册字段级血缘（如果提供）
        if column_lineage:
            for col in column_lineage:
                target_column = col.get("target_column", "").strip()
                source_table = col.get("source_table", "").strip()
                source_column = col.get("source_column", "").strip()
                transform_type = col.get("transform_type", "DIRECT").strip()
                transform_expr = col.get("transform_expr", "").strip() or None

                if not all([target_column, source_table, source_column]):
                    continue

                # 查找对应的表级血缘 ID
                cursor.execute(
                    """SELECT id FROM data_lineage
                       WHERE target_table = %s AND source_table = %s AND is_active = TRUE""",
                    [target_table, source_table]
                )
                table_lineage = cursor.fetchone()
                table_lineage_id = table_lineage['id'] if table_lineage else None

                # 检查是否已存在
                cursor.execute(
                    """SELECT id FROM column_lineage
                       WHERE target_table = %s AND target_column = %s
                         AND source_table = %s AND source_column = %s""",
                    [target_table, target_column, source_table, source_column]
                )
                existing_col = cursor.fetchone()

                if existing_col:
                    cursor.execute(
                        """UPDATE column_lineage
                           SET transform_type = %s, transform_expr = %s,
                               table_lineage_id = %s
                           WHERE id = %s""",
                        [transform_type, transform_expr, table_lineage_id, existing_col['id']]
                    )
                    registered_column_lineage.append({
                        "action": "updated",
                        "target_column": target_column,
                        "source_column": f"{source_table}.{source_column}"
                    })
                else:
                    cursor.execute(
                        """INSERT INTO column_lineage
                           (target_table, target_column, source_table, source_column,
                            transform_type, transform_expr, table_lineage_id, created_time)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                        [target_table, target_column, source_table, source_column,
                         transform_type, transform_expr, table_lineage_id]
                    )
                    registered_column_lineage.append({
                        "action": "inserted",
                        "target_column": target_column,
                        "source_column": f"{source_table}.{source_column}"
                    })

        conn.commit()

        return {
            "target_table": target_table,
            "table_lineage": registered_table_lineage,
            "column_lineage": registered_column_lineage,
            "summary": {
                "table_lineage_count": len(registered_table_lineage),
                "column_lineage_count": len(registered_column_lineage)
            }
        }

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def search_lineage_upstream(table_name: str, depth: int = 1, include_columns: bool = False) -> dict:
    """
    查询表的上游依赖（我依赖谁）

    Args:
        table_name: 表完整名，如 'dm.dmm_sac_loan_prod_daily'
        depth: 递归深度，默认 1（仅直接依赖）
        include_columns: 是否包含字段级血缘

    Returns:
        上游依赖信息
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 递归查询上游表
        upstream_tables = []
        visited = set()
        to_visit = [(table_name, 0)]

        while to_visit:
            current_table, current_depth = to_visit.pop(0)
            if current_table in visited or current_depth >= depth:
                continue
            visited.add(current_table)

            cursor.execute(
                """SELECT source_table, join_type, relation_type, etl_logic_summary,
                          etl_script_path, created_by
                   FROM data_lineage
                   WHERE target_table = %s AND is_active = TRUE""",
                [current_table]
            )
            sources = cursor.fetchall()

            for src in sources:
                upstream_tables.append({
                    "target": current_table,
                    "source": src['source_table'],
                    "join_type": src['join_type'],
                    "relation_type": src['relation_type'],
                    "logic_summary": src['etl_logic_summary'],
                    "depth": current_depth + 1
                })
                if current_depth + 1 < depth:
                    to_visit.append((src['source_table'], current_depth + 1))

        # 查询字段级血缘（如果需要）
        column_lineage = []
        if include_columns:
            cursor.execute(
                """SELECT target_column, source_table, source_column,
                          transform_type, transform_expr
                   FROM column_lineage
                   WHERE target_table = %s""",
                [table_name]
            )
            column_lineage = [dict(row) for row in cursor.fetchall()]

        return {
            "table": table_name,
            "upstream_tables": upstream_tables,
            "column_lineage": column_lineage if include_columns else None,
            "total_upstream": len(upstream_tables)
        }

    finally:
        cursor.close()
        conn.close()


def search_lineage_downstream(table_name: str, depth: int = 1, include_columns: bool = False) -> dict:
    """
    查询表的下游影响（谁依赖我）

    Args:
        table_name: 表完整名
        depth: 递归深度，默认 1（仅直接影响）
        include_columns: 是否包含字段级血缘

    Returns:
        下游影响信息
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 递归查询下游表
        downstream_tables = []
        visited = set()
        to_visit = [(table_name, 0)]

        while to_visit:
            current_table, current_depth = to_visit.pop(0)
            if current_table in visited or current_depth >= depth:
                continue
            visited.add(current_table)

            cursor.execute(
                """SELECT target_table, join_type, relation_type, etl_logic_summary
                   FROM data_lineage
                   WHERE source_table = %s AND is_active = TRUE""",
                [current_table]
            )
            targets = cursor.fetchall()

            for tgt in targets:
                downstream_tables.append({
                    "source": current_table,
                    "target": tgt['target_table'],
                    "join_type": tgt['join_type'],
                    "relation_type": tgt['relation_type'],
                    "logic_summary": tgt['etl_logic_summary'],
                    "depth": current_depth + 1
                })
                if current_depth + 1 < depth:
                    to_visit.append((tgt['target_table'], current_depth + 1))

        # 查询字段级影响（如果需要）
        column_impact = []
        if include_columns:
            cursor.execute(
                """SELECT target_table, target_column, source_column, transform_type
                   FROM column_lineage
                   WHERE source_table = %s""",
                [table_name]
            )
            column_impact = [dict(row) for row in cursor.fetchall()]

        return {
            "table": table_name,
            "downstream_tables": downstream_tables,
            "column_impact": column_impact if include_columns else None,
            "total_downstream": len(downstream_tables)
        }

    finally:
        cursor.close()
        conn.close()


def register_indicator(indicators: list[dict], created_by: str = "auto") -> dict:
    """
    将新指标注册到指标库。支持单条和批量注册（写入 PostgreSQL）。
    注册前会检查是否已存在同名指标，避免重复入库。

    Args:
        indicators: 指标列表，每条包含:
            必填字段:
            - indicator_code (str): 指标编码，如 'IDX_LOAN_001'
            - indicator_name (str): 业务指标名称，如 '当日放款金额'
            - indicator_english_name (str): 英文名/物理字段名，如 'td_loan_amt'
            - indicator_category (str): 指标分类，如 '原子指标'/'派生指标'/'复合指标'
            - business_domain (str): 业务域，如 '贷款'/'风控'/'营销'
            - data_type (str): 数据类型，如 'DECIMAL'/'BIGINT'/'VARCHAR'
            - standard_type (str): 标准类型，如 '金额'/'数量'/'比率'
            - update_frequency (str): 更新频率，如 '日'/'小时'/'实时'
            - status (str): 状态，如 '生效'/'草稿'/'废弃'

            可选字段:
            - indicator_alias (str): 指标别名
            - statistical_caliber (str): 业务口径描述
            - calculation_logic (str): 取值逻辑/计算公式
            - data_source (str): 数据来源表
            - value_domain (str): 值域说明
            - sensitive (str): 敏感级别

        created_by: 创建人标识（同时作为 it_owner 和 business_owner）

    Returns:
        注册结果摘要
    """
    conn = get_pg_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        registered = []
        skipped = []
        failed = []

        required_fields = [
            "indicator_code", "indicator_name", "indicator_english_name",
            "indicator_category", "business_domain", "data_type",
            "standard_type", "update_frequency", "status"
        ]

        for ind in indicators:
            # 提取必填字段
            code = ind.get("indicator_code", "").strip()
            name = ind.get("indicator_name", "").strip()
            english_name = ind.get("indicator_english_name", "").strip()
            category = ind.get("indicator_category", "").strip()
            domain = ind.get("business_domain", "").strip()
            data_type = ind.get("data_type", "").strip()
            standard_type = ind.get("standard_type", "").strip()
            frequency = ind.get("update_frequency", "").strip()
            status = ind.get("status", "生效").strip()

            # 提取可选字段
            alias = ind.get("indicator_alias", "").strip() or None
            caliber = ind.get("statistical_caliber", "").strip() or None
            logic = ind.get("calculation_logic", "").strip() or None
            source = ind.get("data_source", "").strip() or None
            value_domain = ind.get("value_domain", "").strip() or None
            sensitive = ind.get("sensitive", "").strip() or None

            # 负责人
            it_owner = ind.get("it_owner", created_by).strip()
            business_owner = ind.get("business_owner", created_by).strip()

            # 校验必填字段
            missing = [f for f in required_fields if not ind.get(f, "").strip()]
            if missing:
                failed.append({
                    "indicator_name": name or "(空)",
                    "reason": f"缺少必填字段: {', '.join(missing)}"
                })
                continue

            # 检查是否已存在同名或同编码指标
            cursor.execute(
                """SELECT id, indicator_code, indicator_name, data_source, statistical_caliber
                   FROM indicator_registry
                   WHERE indicator_name = %s OR indicator_code = %s""",
                [name, code]
            )
            existing = cursor.fetchone()

            if existing:
                skipped.append({
                    "indicator_name": name,
                    "indicator_code": code,
                    "existing_code": existing["indicator_code"],
                    "existing_source": existing["data_source"],
                    "existing_caliber": existing["statistical_caliber"],
                    "reason": "同名或同编码指标已存在"
                })
                continue

            # 插入新指标
            cursor.execute(
                """
                INSERT INTO indicator_registry (
                    indicator_code, indicator_name, indicator_alias, indicator_english_name,
                    indicator_category, business_domain, statistical_caliber, data_type,
                    standard_type, value_domain, sensitive, calculation_logic, data_source,
                    update_frequency, it_owner, business_owner, create_time, update_time, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s
                )
                """,
                [code, name, alias, english_name, category, domain, caliber, data_type,
                 standard_type, value_domain, sensitive, logic, source, frequency,
                 it_owner, business_owner, status]
            )
            registered.append({
                "indicator_code": code,
                "indicator_name": name,
                "indicator_english_name": english_name,
                "data_source": source,
                "statistical_caliber": caliber
            })

        conn.commit()

        return {
            "registered": registered,
            "skipped": skipped,
            "failed": failed,
            "summary": {
                "total": len(indicators),
                "registered": len(registered),
                "skipped": len(skipped),
                "failed": len(failed)
            }
        }

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


# ============== MCP Server 定义 ==============

server = Server("hive-metadata-search")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """列出可用工具"""
    return [
        Tool(
            name="search_table",
            description="按表名搜索 Hive 表。支持模糊匹配，可限定数据库。",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "表名关键词，支持模糊匹配，如 'order'、'user'"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "限定数据库名，如 'ods'、'dwd'、'dws'、'ads'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "返回结果数量，默认 10",
                        "default": 10
                    }
                },
                "required": ["keyword"]
            }
        ),
        Tool(
            name="search_by_comment",
            description="按业务术语（注释）搜索表和字段。用于根据中文业务名称查找对应的物理表和字段。",
            inputSchema={
                "type": "object",
                "properties": {
                    "term": {
                        "type": "string",
                        "description": "业务术语，如 '申请时间'、'放款金额'、'用户ID'"
                    },
                    "search_scope": {
                        "type": "string",
                        "enum": ["table", "column", "all"],
                        "description": "搜索范围: table(仅表注释), column(仅字段注释), all(全部)",
                        "default": "all"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "返回结果数量，默认 10",
                        "default": 10
                    }
                },
                "required": ["term"]
            }
        ),
        Tool(
            name="get_table_detail",
            description="获取表的详细信息，包括表注释、字段列表、分区键、数据量、负责人等。",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name_full": {
                        "type": "string",
                        "description": "完整表名，如 'ods.ods_order_info' 或 'dwd.dwd_order_detail'"
                    }
                },
                "required": ["table_name_full"]
            }
        ),
        Tool(
            name="list_columns",
            description="获取表的字段列表，返回字段名、类型、注释。",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name_full": {
                        "type": "string",
                        "description": "完整表名"
                    }
                },
                "required": ["table_name_full"]
            }
        ),
        Tool(
            name="search_word_root",
            description="搜索词根表，获取标准词根（english_abbr）用于字段命名。在生成 DDL 时，必须通过此工具查询词根，确保字段命名符合规范。",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "搜索关键词，支持中文或英文，如 '放款'、'loan'、'金额'、'amt'"
                    },
                    "tag": {
                        "type": "string",
                        "description": "词根分类标签筛选（可选），如 'BIZ_ENTITY'(业务实体)、'CATEGORY_WORD'(分类词)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "返回结果数量，默认 20",
                        "default": 20
                    }
                },
                "required": ["keyword"]
            }
        ),
        Tool(
            name="search_existing_indicators",
            description="在设计新报表前，优先查询指标库，检查目标指标是否已经被计算过。遵循'复用优先'原则，避免重复造轮子。返回指标的业务口径(statistical_caliber)和取值逻辑(calculation_logic)。",
            inputSchema={
                "type": "object",
                "properties": {
                    "metric_name": {
                        "type": "string",
                        "description": "业务指标名称，如 '复购率', 'GMV', '日销售额', '放款金额'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "返回结果数量，默认 10",
                        "default": 10
                    }
                },
                "required": ["metric_name"]
            }
        ),
        Tool(
            name="register_lineage",
            description="注册表级和字段级血缘关系。在 ETL 开发完成后自动调用，记录目标表与源表的依赖关系，支持后续的影响分析和数据溯源。",
            inputSchema={
                "type": "object",
                "properties": {
                    "target_table": {
                        "type": "string",
                        "description": "目标表完整名，如 'dm.dmm_sac_loan_prod_daily'"
                    },
                    "source_tables": {
                        "type": "array",
                        "description": "源表列表",
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_table": {
                                    "type": "string",
                                    "description": "源表完整名"
                                },
                                "join_type": {
                                    "type": "string",
                                    "description": "JOIN 类型: 'FROM', 'LEFT JOIN', 'INNER JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN'"
                                },
                                "relation_type": {
                                    "type": "string",
                                    "description": "关系类型: 'ETL'(默认), 'VIEW', 'MANUAL'",
                                    "default": "ETL"
                                }
                            },
                            "required": ["source_table"]
                        }
                    },
                    "etl_script_path": {
                        "type": "string",
                        "description": "ETL 脚本路径（可选）"
                    },
                    "etl_logic_summary": {
                        "type": "string",
                        "description": "ETL 逻辑摘要，如 '按产品维度聚合放款明细'（可选）"
                    },
                    "column_lineage": {
                        "type": "array",
                        "description": "字段级血缘列表（可选）",
                        "items": {
                            "type": "object",
                            "properties": {
                                "target_column": {
                                    "type": "string",
                                    "description": "目标字段名"
                                },
                                "source_table": {
                                    "type": "string",
                                    "description": "源表完整名"
                                },
                                "source_column": {
                                    "type": "string",
                                    "description": "源字段名"
                                },
                                "transform_type": {
                                    "type": "string",
                                    "description": "转换类型: 'DIRECT', 'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'CASE', 'CUSTOM'"
                                },
                                "transform_expr": {
                                    "type": "string",
                                    "description": "转换表达式，如 'SUM(loan_amount)'"
                                }
                            },
                            "required": ["target_column", "source_table", "source_column"]
                        }
                    },
                    "created_by": {
                        "type": "string",
                        "description": "创建人标识，默认 'auto'",
                        "default": "auto"
                    }
                },
                "required": ["target_table", "source_tables"]
            }
        ),
        Tool(
            name="search_lineage_upstream",
            description="查询表的上游依赖（我依赖谁）。用于了解数据来源、追溯数据问题、评估源表变更影响。",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "表完整名，如 'dm.dmm_sac_loan_prod_daily'"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "递归深度，1=仅直接依赖，2=包含二级依赖，默认 1",
                        "default": 1
                    },
                    "include_columns": {
                        "type": "boolean",
                        "description": "是否包含字段级血缘，默认 false",
                        "default": False
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="search_lineage_downstream",
            description="查询表的下游影响（谁依赖我）。用于评估表变更影响范围、通知下游用户、规划数据迁移。",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "表完整名，如 'dwd.dwd_loan_detail'"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "递归深度，1=仅直接影响，2=包含二级影响，默认 1",
                        "default": 1
                    },
                    "include_columns": {
                        "type": "boolean",
                        "description": "是否包含字段级影响，默认 false",
                        "default": False
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="register_indicator",
            description="将新指标注册到指标库。ETL 开发完成后，对新产生的公共指标执行入库，闭环'复用优先'流程。注册前会自动检查重复（按指标名称或编码）。",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicators": {
                        "type": "array",
                        "description": "待注册的指标列表",
                        "items": {
                            "type": "object",
                            "properties": {
                                "indicator_code": {
                                    "type": "string",
                                    "description": "指标编码，如 'IDX_LOAN_001'"
                                },
                                "indicator_name": {
                                    "type": "string",
                                    "description": "业务指标名称，如 '当日放款金额'"
                                },
                                "indicator_english_name": {
                                    "type": "string",
                                    "description": "英文名/物理字段名，如 'td_loan_amt'"
                                },
                                "indicator_alias": {
                                    "type": "string",
                                    "description": "指标别名（可选）"
                                },
                                "indicator_category": {
                                    "type": "string",
                                    "description": "指标分类: '原子指标'/'派生指标'/'复合指标'"
                                },
                                "business_domain": {
                                    "type": "string",
                                    "description": "业务域，如 '贷款'/'风控'/'营销'"
                                },
                                "statistical_caliber": {
                                    "type": "string",
                                    "description": "业务口径描述（可选）"
                                },
                                "calculation_logic": {
                                    "type": "string",
                                    "description": "取值逻辑/计算公式（可选）"
                                },
                                "data_source": {
                                    "type": "string",
                                    "description": "数据来源表，如 'dm.dmm_sac_loan_prod_daily'（可选）"
                                },
                                "data_type": {
                                    "type": "string",
                                    "description": "数据类型: 'DECIMAL'/'BIGINT'/'VARCHAR' 等"
                                },
                                "standard_type": {
                                    "type": "string",
                                    "description": "标准类型，如 '金额'/'数量'/'比率'"
                                },
                                "update_frequency": {
                                    "type": "string",
                                    "description": "更新频率: '日'/'小时'/'实时'"
                                },
                                "status": {
                                    "type": "string",
                                    "description": "状态: '生效'/'草稿'/'废弃'，默认 '生效'",
                                    "default": "生效"
                                },
                                "it_owner": {
                                    "type": "string",
                                    "description": "IT负责人（可选，默认使用 created_by）"
                                },
                                "business_owner": {
                                    "type": "string",
                                    "description": "业务负责人（可选，默认使用 created_by）"
                                }
                            },
                            "required": [
                                "indicator_code", "indicator_name", "indicator_english_name",
                                "indicator_category", "business_domain", "data_type",
                                "standard_type", "update_frequency"
                            ]
                        }
                    },
                    "created_by": {
                        "type": "string",
                        "description": "创建人标识，默认 'auto'",
                        "default": "auto"
                    }
                },
                "required": ["indicators"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """执行工具调用"""
    try:
        if name == "search_table":
            results = search_table(
                keyword=arguments["keyword"],
                schema_name=arguments.get("schema_name"),
                limit=arguments.get("limit", 10)
            )

            if not results:
                return [TextContent(
                    type="text",
                    text=f"未找到包含 '{arguments['keyword']}' 的表"
                )]

            # 格式化输出
            output = f"## 搜索结果: \"{arguments['keyword']}\"\n\n"
            output += f"找到 {len(results)} 个匹配的表:\n\n"
            output += "| 表名 | 数据库 | 注释 | 数据量 | 行数 | 字段数 |\n"
            output += "|-----|-------|------|-------|------|-------|\n"

            for row in results:
                comment = row.get('table_comment', '-') or '-'
                comment_display = comment[:30] + '...' if len(comment) > 30 else comment
                row_cnt = row.get('tbl_row_cnt')
                row_cnt_display = f"{row_cnt:,}" if row_cnt else '-'
                output += f"| {row['table_name_full']} | {row.get('schema_name', '-')} | {comment_display} | {row.get('total_data_size_display', '-')} | {row_cnt_display} | {row.get('column_cnt', '-')} |\n"

            if len(results) > 1:
                output += f"\n**提示**: 找到多个匹配结果，请使用 `get_table_detail` 查看具体表的详情，或提供更精确的关键词。"

            return [TextContent(type="text", text=output)]

        elif name == "search_by_comment":
            results = search_by_comment(
                term=arguments["term"],
                search_scope=arguments.get("search_scope", "all"),
                limit=arguments.get("limit", 10)
            )

            if not results:
                return [TextContent(
                    type="text",
                    text=f"未找到包含 '{arguments['term']}' 的表或字段"
                )]

            output = f"## 业务术语搜索: \"{arguments['term']}\"\n\n"

            for row in results:
                match_type = row.get("match_type", "unknown")
                output += f"### {row['table_name_full']}\n"
                output += f"- **匹配类型**: {'表注释' if match_type == 'table' else '字段注释'}\n"
                output += f"- **表注释**: {row.get('table_comment', '-')}\n"

                if row.get("matched_columns"):
                    output += "- **匹配字段**:\n"
                    for col in row["matched_columns"]:
                        if col.get("raw"):
                            output += f"  - {col['raw'][:100]}...\n"
                        else:
                            output += f"  - `{col.get('name', '?')}` ({col.get('type', '?')}): {col.get('comment', '-')}\n"

                output += "\n"

            return [TextContent(type="text", text=output)]

        elif name == "get_table_detail":
            result = get_table_detail(arguments["table_name_full"])

            if not result:
                return [TextContent(
                    type="text",
                    text=f"未找到表: {arguments['table_name_full']}"
                )]

            output = f"## 表详情: {result['table_name_full']}\n\n"
            output += f"- **表注释**: {result.get('table_comment', '-') or '-'}\n"
            output += f"- **数据库**: {result.get('schema_name', '-')}\n"
            output += f"- **表名**: {result.get('table_name', '-')}\n"
            output += f"- **分区键**: {result.get('partition_key', '-')}\n"
            output += f"- **数据量**: {result.get('total_data_size_display', '-')}\n"
            row_cnt = result.get('tbl_row_cnt')
            output += f"- **行数**: {row_cnt:,}\n" if row_cnt else "- **行数**: -\n"
            output += f"- **字段数**: {result.get('column_cnt', '-')}\n"
            output += f"- **分区数**: {result.get('partition_cnt', '-')}\n"
            output += f"- **文件数**: {result.get('file_cnt', '-')}\n"
            output += f"- **存储格式**: {result.get('tbl_strg_format', '-')}\n"
            output += f"- **表类型**: {result.get('tbl_type', '-')}\n"

            columns = result.get("columns", [])
            if columns:
                output += "\n### 字段列表\n\n"
                output += "| 字段名 | 类型 | 注释 |\n"
                output += "|-------|-----|------|\n"
                for col in columns:
                    if isinstance(col, dict):
                        output += f"| {col.get('name', '?')} | {col.get('type', '?')} | {col.get('comment', '-') or '-'} |\n"

            # 显示分区信息
            partitions = result.get("partitions", [])
            if partitions and len(partitions) <= 10:
                output += "\n### 分区列表\n\n"
                output += "| 分区值 | 文件数 | 行数 |\n"
                output += "|-------|-------|------|\n"
                for p in partitions[:10]:
                    if isinstance(p, dict):
                        output += f"| {p.get('partition_value', '?')} | {p.get('numfiles', '-')} | {p.get('numrows', '-')} |\n"
                if len(partitions) > 10:
                    output += f"\n(仅显示前 10 个分区，共 {len(partitions)} 个)\n"

            return [TextContent(type="text", text=output)]

        elif name == "list_columns":
            columns = list_columns(arguments["table_name_full"])

            if not columns:
                return [TextContent(
                    type="text",
                    text=f"未找到表 {arguments['table_name_full']} 或表无字段信息"
                )]

            output = f"## 字段列表: {arguments['table_name_full']}\n\n"
            output += "| 字段名 | 类型 | 注释 |\n"
            output += "|-------|-----|------|\n"

            for col in columns:
                if isinstance(col, dict):
                    if col.get("raw"):
                        output += f"| (原始数据) | - | {col['raw'][:50]}... |\n"
                    else:
                        output += f"| {col.get('name', '?')} | {col.get('type', '?')} | {col.get('comment', '-')} |\n"

            return [TextContent(type="text", text=output)]

        elif name == "search_word_root":
            results = search_word_root(
                keyword=arguments["keyword"],
                tag=arguments.get("tag"),
                limit=arguments.get("limit", 20)
            )

            if not results:
                return [TextContent(
                    type="text",
                    text=f"## 词根查询: \"{arguments['keyword']}\"\n\n"
                         f"未找到匹配的词根。请尝试其他关键词，或使用通用英文缩写并在 COMMENT 中标注'（新词根，待入库）'。"
                )]

            output = f"## 词根查询: \"{arguments['keyword']}\"\n\n"
            output += f"找到 {len(results)} 个匹配词根:\n\n"
            output += "| 缩写(用于命名) | 中文名 | 英文全称 | 别名 | 分类标签 |\n"
            output += "|---------------|--------|---------|------|----------|\n"

            for row in results:
                output += f"| `{row.get('english_abbr', '-')}` | {row.get('chinese_name', '-')} | {row.get('english_name', '-')} | {row.get('alias', '-') or '-'} | {row.get('tag', '-')} |\n"

            output += "\n**使用方式**: 用 `缩写` 列的值按 `{布尔}_{时间}_{聚合}_{业务主题}_{分类}` 顺序组装字段名。\n"

            return [TextContent(type="text", text=output)]

        elif name == "search_existing_indicators":
            results = search_existing_indicators(
                metric_name=arguments["metric_name"],
                limit=arguments.get("limit", 10)
            )

            if not results:
                return [TextContent(
                    type="text",
                    text=f"## 指标库查询: \"{arguments['metric_name']}\"\n\n"
                         f"未找到已存在的指标。建议从 ODS/DWD 层开始开发。\n\n"
                         f"可使用 `search_by_comment` 搜索相关表和字段。"
                )]

            output = f"## 指标库查询: \"{arguments['metric_name']}\"\n\n"
            output += f"找到 {len(results)} 个已存在的指标:\n\n"

            for i, row in enumerate(results, 1):
                match_label = {
                    "perfect": "精确匹配",
                    "high": "高度匹配",
                    "partial": "部分匹配"
                }.get(row.get("match_type"), "匹配")

                output += f"### {i}. {row['indicator_name']} ({match_label})\n\n"
                output += f"- **指标编码**: `{row.get('indicator_code', '-')}`\n"
                output += f"- **英文名/字段名**: `{row.get('indicator_english_name', '-')}`\n"
                if row.get('indicator_alias'):
                    output += f"- **别名**: {row['indicator_alias']}\n"
                output += f"- **指标分类**: {row.get('indicator_category', '-')}\n"
                output += f"- **业务域**: {row.get('business_domain', '-')}\n"
                output += f"- **数据来源**: `{row.get('data_source', '-') or '-'}`\n"
                output += f"- **业务口径**: {row.get('statistical_caliber', '-') or '-'}\n"
                output += f"- **取值逻辑**: {row.get('calculation_logic', '-') or '-'}\n"
                output += f"- **数据类型**: {row.get('data_type', '-')}\n"
                output += f"- **IT负责人**: {row.get('it_owner', '-')}\n"
                output += f"- **状态**: {row.get('status', '-')}\n"
                output += "\n"

            output += "---\n\n"
            output += "**请确认是否复用:**\n\n"
            output += "- **(A) 是，直接复用** - 口径一致，将直接 SELECT 该字段\n"
            output += "- **(B) 否，需要重新计算** - 口径不符，从 ODS/DWD 层重新开发\n"

            return [TextContent(type="text", text=output)]

        elif name == "register_lineage":
            result = register_lineage(
                target_table=arguments["target_table"],
                source_tables=arguments["source_tables"],
                etl_script_path=arguments.get("etl_script_path"),
                etl_logic_summary=arguments.get("etl_logic_summary"),
                column_lineage=arguments.get("column_lineage"),
                created_by=arguments.get("created_by", "auto")
            )

            summary = result["summary"]
            output = f"## 血缘注册结果\n\n"
            output += f"**目标表**: `{result['target_table']}`\n\n"

            if result["table_lineage"]:
                output += "### 表级血缘\n\n"
                output += "| 操作 | 源表 | JOIN 类型 |\n"
                output += "|-----|------|----------|\n"
                for tl in result["table_lineage"]:
                    action_label = "更新" if tl["action"] == "updated" else "新增"
                    output += f"| {action_label} | `{tl['source_table']}` | {tl['join_type']} |\n"
                output += "\n"

            if result["column_lineage"]:
                output += "### 字段级血缘\n\n"
                output += "| 操作 | 目标字段 | 来源 |\n"
                output += "|-----|---------|------|\n"
                for cl in result["column_lineage"]:
                    action_label = "更新" if cl["action"] == "updated" else "新增"
                    output += f"| {action_label} | `{cl['target_column']}` | `{cl['source_column']}` |\n"
                output += "\n"

            output += f"**汇总**: 表级血缘 {summary['table_lineage_count']} 条，字段级血缘 {summary['column_lineage_count']} 条\n"

            return [TextContent(type="text", text=output)]

        elif name == "search_lineage_upstream":
            result = search_lineage_upstream(
                table_name=arguments["table_name"],
                depth=arguments.get("depth", 1),
                include_columns=arguments.get("include_columns", False)
            )

            output = f"## 上游血缘: `{result['table']}`\n\n"

            if not result["upstream_tables"]:
                output += "未找到上游依赖记录。可能原因:\n"
                output += "- 该表是源头表（ODS 层）\n"
                output += "- 血缘关系尚未注册\n"
            else:
                output += f"找到 **{result['total_upstream']}** 个上游依赖:\n\n"

                # 按深度分组显示
                by_depth = {}
                for up in result["upstream_tables"]:
                    d = up["depth"]
                    if d not in by_depth:
                        by_depth[d] = []
                    by_depth[d].append(up)

                for depth in sorted(by_depth.keys()):
                    output += f"### 第 {depth} 层依赖\n\n"
                    output += "| 源表 | JOIN 类型 | 关系 | 逻辑摘要 |\n"
                    output += "|------|----------|------|----------|\n"
                    for up in by_depth[depth]:
                        logic = up.get("logic_summary") or "-"
                        logic_display = logic[:30] + "..." if len(logic) > 30 else logic
                        output += f"| `{up['source']}` | {up['join_type']} | {up['relation_type']} | {logic_display} |\n"
                    output += "\n"

            # 字段级血缘
            if result.get("column_lineage"):
                output += "### 字段级血缘\n\n"
                output += "| 目标字段 | 来源表.字段 | 转换类型 | 表达式 |\n"
                output += "|---------|------------|---------|--------|\n"
                for cl in result["column_lineage"]:
                    expr = cl.get("transform_expr") or "-"
                    expr_display = expr[:20] + "..." if len(expr) > 20 else expr
                    output += f"| `{cl['target_column']}` | `{cl['source_table']}.{cl['source_column']}` | {cl['transform_type']} | {expr_display} |\n"
                output += "\n"

            return [TextContent(type="text", text=output)]

        elif name == "search_lineage_downstream":
            result = search_lineage_downstream(
                table_name=arguments["table_name"],
                depth=arguments.get("depth", 1),
                include_columns=arguments.get("include_columns", False)
            )

            output = f"## 下游影响: `{result['table']}`\n\n"

            if not result["downstream_tables"]:
                output += "未找到下游依赖记录。可能原因:\n"
                output += "- 该表是末端表（DA/报表层）\n"
                output += "- 血缘关系尚未注册\n"
            else:
                output += f"找到 **{result['total_downstream']}** 个下游表会受影响:\n\n"

                # 按深度分组显示
                by_depth = {}
                for down in result["downstream_tables"]:
                    d = down["depth"]
                    if d not in by_depth:
                        by_depth[d] = []
                    by_depth[d].append(down)

                for depth in sorted(by_depth.keys()):
                    output += f"### 第 {depth} 层影响\n\n"
                    output += "| 下游表 | JOIN 类型 | 关系 | 逻辑摘要 |\n"
                    output += "|-------|----------|------|----------|\n"
                    for down in by_depth[depth]:
                        logic = down.get("logic_summary") or "-"
                        logic_display = logic[:30] + "..." if len(logic) > 30 else logic
                        output += f"| `{down['target']}` | {down['join_type']} | {down['relation_type']} | {logic_display} |\n"
                    output += "\n"

                output += "**⚠️ 变更提醒**: 修改此表前，请评估对上述下游表的影响，并通知相关负责人。\n"

            # 字段级影响
            if result.get("column_impact"):
                output += "\n### 字段级影响\n\n"
                output += "| 下游表 | 下游字段 | 本表字段 | 转换类型 |\n"
                output += "|-------|---------|---------|----------|\n"
                for ci in result["column_impact"]:
                    output += f"| `{ci['target_table']}` | `{ci['target_column']}` | `{ci['source_column']}` | {ci['transform_type']} |\n"
                output += "\n"

            return [TextContent(type="text", text=output)]

        elif name == "register_indicator":
            result = register_indicator(
                indicators=arguments["indicators"],
                created_by=arguments.get("created_by", "auto")
            )

            summary = result["summary"]
            output = f"## 指标注册结果\n\n"
            output += f"提交 {summary['total']} 条，"
            output += f"成功 {summary['registered']} 条，"
            output += f"跳过 {summary['skipped']} 条，"
            output += f"失败 {summary['failed']} 条\n\n"

            if result["registered"]:
                output += "### 已注册\n\n"
                output += "| 指标编码 | 指标名称 | 英文名/字段名 | 数据来源 | 业务口径 |\n"
                output += "|---------|---------|--------------|---------|----------|\n"
                for r in result["registered"]:
                    output += f"| `{r['indicator_code']}` | {r['indicator_name']} | `{r['indicator_english_name']}` | `{r.get('data_source') or '-'}` | {r.get('statistical_caliber') or '-'} |\n"
                output += "\n"

            if result["skipped"]:
                output += "### 已跳过（同名或同编码指标已存在）\n\n"
                output += "| 指标编码 | 指标名称 | 已有编码 | 已有来源 | 已有口径 |\n"
                output += "|---------|---------|---------|---------|----------|\n"
                for s in result["skipped"]:
                    output += f"| `{s.get('indicator_code', '-')}` | {s['indicator_name']} | `{s.get('existing_code', '-')}` | `{s.get('existing_source') or '-'}` | {s.get('existing_caliber') or '-'} |\n"
                output += "\n"
                output += "**如需更新已有指标，请联系指标管理员手动修改。**\n\n"

            if result["failed"]:
                output += "### 失败\n\n"
                for f in result["failed"]:
                    output += f"- **{f['indicator_name']}**: {f['reason']}\n"
                output += "\n"

            return [TextContent(type="text", text=output)]

        else:
            return [TextContent(type="text", text=f"未知工具: {name}")]

    except MySQLError as e:
        return [TextContent(type="text", text=f"MySQL 数据库错误: {str(e)}")]
    except PGError as e:
        return [TextContent(type="text", text=f"PostgreSQL 数据库错误: {str(e)}")]
    except Exception as e:
        return [TextContent(type="text", text=f"执行错误: {str(e)}")]


async def main():
    """启动 MCP Server"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
