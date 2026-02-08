#!/usr/bin/env python3
"""
SQL 解析器 - 提取血缘和指标信息
"""
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ColumnLineage:
    """字段级血缘"""
    target_column: str
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    transform_type: str = 'DIRECT'  # DIRECT, SUM, COUNT, AVG, MAX, MIN, CASE, CUSTOM
    transform_expr: Optional[str] = None


@dataclass
class TableLineage:
    """表级血缘"""
    source_table: str
    join_type: str = 'FROM'  # FROM, LEFT JOIN, INNER JOIN, RIGHT JOIN, FULL JOIN
    join_condition: Optional[str] = None


@dataclass
class Indicator:
    """指标定义"""
    indicator_english_name: str
    indicator_name: str  # 中文名，需用户确认
    calculation_logic: str
    standard_type: str  # 金额/数量/比率
    indicator_category: str  # 原子指标/派生指标/复合指标
    data_type: str = 'DECIMAL'
    business_domain: str = ''  # 从表名推断
    update_frequency: str = '日'  # 从分区字段推断


@dataclass
class SQLAnalysisResult:
    """SQL 分析结果"""
    target_table: str
    sql_file: str
    table_lineage: List[TableLineage] = field(default_factory=list)
    column_lineage: List[ColumnLineage] = field(default_factory=list)
    indicators: List[Indicator] = field(default_factory=list)
    etl_logic_summary: str = ''


class SQLParser:
    """SQL 解析器"""

    def __init__(self, sql_content: str, sql_file: str = '', mcp_client=None):
        """
        Args:
            sql_content: SQL 脚本内容
            sql_file: 文件路径
            mcp_client: MCP 客户端（用于查询元数据）
        """
        self.sql_content = self._normalize_sql(sql_content)
        self.sql_file = sql_file
        self.mcp_client = mcp_client
        self.result = SQLAnalysisResult(
            target_table='',
            sql_file=sql_file
        )
        self.target_columns_from_metadata = []  # 从元数据获取的真实字段列表

    # 聚合函数模式
    AGG_PATTERNS = {
        'SUM': (r'SUM\s*\(\s*([^)]+)\s*\)', 'SUM'),
        'COUNT': (r'COUNT\s*\(\s*DISTINCT\s+([^)]+)\s*\)', 'COUNT'),
        'COUNT_ALL': (r'COUNT\s*\(\s*([^)]+)\s*\)', 'COUNT'),
        'AVG': (r'AVG\s*\(\s*([^)]+)\s*\)', 'AVG'),
        'MAX': (r'MAX\s*\(\s*([^)]+)\s*\)', 'MAX'),
        'MIN': (r'MIN\s*\(\s*([^)]+)\s*\)', 'MIN'),
    }

    # 标准类型推断规则
    TYPE_INFERENCE = {
        r'amt|amount|money|price|fee': '金额',
        r'cnt|count|num|quantity': '数量',
        r'rate|ratio|percent|pct': '比率',
        r'date|time|dt': '时间',
        r'avg|mean': '平均值',
    }

    # 业务域推断规则（从表名）
    DOMAIN_INFERENCE = {
        r'loan': '贷款',
        r'overdue|repay': '贷后',
        r'customer|user': '客户',
        r'product': '产品',
        r'risk': '风控',
    }

    # (已移除，上面有新的 __init__)

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        """规范化 SQL：去注释、统一空白符"""
        # 去除单行注释
        sql = re.sub(r'--[^\n]*', '', sql)
        # 去除多行注释
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # 统一空白符
        sql = re.sub(r'\s+', ' ', sql)
        return sql.strip()

    def parse(self) -> SQLAnalysisResult:
        """执行完整解析流程"""
        self._extract_target_table()
        self._fetch_target_columns_from_metadata()  # ✨ 新增：查询元数据
        self._extract_table_lineage()
        self._extract_column_lineage()
        self._align_columns_with_metadata()  # ✨ 新增：字段对齐
        self._identify_indicators()
        self._generate_summary()
        return self.result

    def _fetch_target_columns_from_metadata(self):
        """查询目标表的真实字段列表（从元数据）"""
        if not self.mcp_client or not self.result.target_table:
            return

        try:
            # 调用 MCP 工具 list_columns
            columns_result = self.mcp_client.list_columns(self.result.target_table)
            if columns_result and 'columns' in columns_result:
                self.target_columns_from_metadata = [
                    col['column_name'] for col in columns_result['columns']
                    if col['column_name'] not in ('dt', 'create_time', 'update_time')  # 排除分区字段和元信息字段
                ]
        except Exception as e:
            # 元数据查询失败不影响解析流程
            print(f"⚠️ 元数据查询失败: {e}")

    def _align_columns_with_metadata(self):
        """字段对齐：SELECT 别名 → 目标表真实字段名"""
        if not self.target_columns_from_metadata:
            return

        # 策略：位置对齐（Hive INSERT OVERWRITE 默认按位置）
        sql_columns = [cl.target_column for cl in self.result.column_lineage]

        # 检查数量是否匹配
        if len(sql_columns) != len(self.target_columns_from_metadata):
            print(f"⚠️ 字段数量不匹配: SQL {len(sql_columns)} vs 目标表 {len(self.target_columns_from_metadata)}")
            print(f"   SQL 字段: {sql_columns}")
            print(f"   目标表字段: {self.target_columns_from_metadata}")
            # 不强制中断，继续使用 SQL 中的别名
            return

        # 执行位置对齐
        for i, col_lineage in enumerate(self.result.column_lineage):
            real_column_name = self.target_columns_from_metadata[i]
            if col_lineage.target_column != real_column_name:
                print(f"   🔀 字段对齐: {col_lineage.target_column} → {real_column_name}")
                col_lineage.target_column = real_column_name  # 替换为真实字段名

    def _extract_target_table(self):
        """提取目标表"""
        # Hive: INSERT OVERWRITE TABLE db.table PARTITION (dt='...')
        match = re.search(
            r'INSERT\s+OVERWRITE\s+TABLE\s+([a-z_][a-z0-9_.]*)',
            self.sql_content,
            re.I
        )
        if match:
            self.result.target_table = match.group(1)
            return

        # Impala/Doris: INSERT INTO db.table
        match = re.search(
            r'INSERT\s+INTO\s+([a-z_][a-z0-9_.]*)',
            self.sql_content,
            re.I
        )
        if match:
            self.result.target_table = match.group(1)

    def _extract_table_lineage(self):
        """提取表级血缘"""
        # 1. 提取 FROM 子句
        from_match = re.search(
            r'FROM\s+([a-z_][a-z0-9_.]*)',
            self.sql_content,
            re.I
        )
        if from_match:
            self.result.table_lineage.append(
                TableLineage(
                    source_table=from_match.group(1),
                    join_type='FROM'
                )
            )

        # 2. 提取 JOIN 子句
        join_pattern = r'(LEFT|RIGHT|INNER|FULL)?\s*JOIN\s+([a-z_][a-z0-9_.]*)\s+(?:AS\s+)?([a-z_]\w*)?\s+ON\s+([^JOIN|WHERE|GROUP|ORDER|LIMIT]+)'
        for match in re.finditer(join_pattern, self.sql_content, re.I):
            join_type_prefix = match.group(1) or 'INNER'
            table_name = match.group(2)
            join_condition = match.group(4).strip()

            self.result.table_lineage.append(
                TableLineage(
                    source_table=table_name,
                    join_type=f'{join_type_prefix} JOIN',
                    join_condition=join_condition
                )
            )

    def _extract_column_lineage(self):
        """提取字段级血缘"""
        # 提取 SELECT 子句
        select_match = re.search(
            r'SELECT\s+(.*?)\s+FROM',
            self.sql_content,
            re.I | re.DOTALL
        )
        if not select_match:
            return

        select_clause = select_match.group(1)

        # 处理 SELECT * 的情况
        if re.match(r'^\s*\*\s*$', select_clause):
            # TODO: 需要调用 MCP list_columns 获取完整字段列表
            return

        # 分割字段表达式（逗号分隔，但要注意函数内的逗号）
        fields = self._split_select_fields(select_clause)

        for field_expr in fields:
            field_expr = field_expr.strip()
            if not field_expr:
                continue

            # 提取别名
            alias_match = re.search(r'AS\s+([a-z_]\w*)\s*$', field_expr, re.I)
            if alias_match:
                target_column = alias_match.group(1)
                expr_part = field_expr[:alias_match.start()].strip()
            else:
                # 没有 AS，取最后一个标识符
                tokens = re.findall(r'[a-z_]\w*', field_expr, re.I)
                target_column = tokens[-1] if tokens else ''
                expr_part = field_expr

            if not target_column:
                continue

            # 分析转换类型
            lineage = self._analyze_field_expression(target_column, expr_part)
            self.result.column_lineage.append(lineage)

    @staticmethod
    def _split_select_fields(select_clause: str) -> List[str]:
        """分割 SELECT 字段列表（处理函数内的逗号）"""
        fields = []
        current_field = ''
        paren_depth = 0

        for char in select_clause:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                fields.append(current_field)
                current_field = ''
                continue
            current_field += char

        if current_field:
            fields.append(current_field)

        return fields

    def _analyze_field_expression(self, target_column: str, expr: str) -> ColumnLineage:
        """分析字段表达式，识别转换类型"""
        expr_upper = expr.upper()

        # 1. 窗口函数检测（优先级最高，因为窗口函数可能包含聚合）
        if 'OVER' in expr_upper and '(' in expr_upper:
            return ColumnLineage(
                target_column=target_column,
                transform_type='CUSTOM',
                transform_expr=expr
            )

        # 2. 聚合函数检测
        for agg_name, (pattern, transform_type) in self.AGG_PATTERNS.items():
            match = re.search(pattern, expr, re.I)
            if match:
                inner_expr = match.group(1).strip()
                source_info = self._extract_source_column(inner_expr)
                return ColumnLineage(
                    target_column=target_column,
                    source_table=source_info['table'],
                    source_column=source_info['column'],
                    transform_type=transform_type,
                    transform_expr=expr
                )

        # 3. CASE WHEN 检测
        if 'CASE' in expr_upper and 'WHEN' in expr_upper:
            return ColumnLineage(
                target_column=target_column,
                transform_type='CASE',
                transform_expr=expr
            )

        # 4. 算术运算检测
        if re.search(r'[+\-*/]', expr):
            return ColumnLineage(
                target_column=target_column,
                transform_type='CUSTOM',
                transform_expr=expr
            )

        # 4. 直接映射
        source_info = self._extract_source_column(expr)
        return ColumnLineage(
            target_column=target_column,
            source_table=source_info['table'],
            source_column=source_info['column'],
            transform_type='DIRECT',
            transform_expr=None
        )

    @staticmethod
    def _extract_source_column(expr: str) -> Dict[str, Optional[str]]:
        """从表达式中提取源表和源字段"""
        # 匹配 table_alias.column_name 或 column_name
        match = re.search(r'([a-z_]\w*)\.([a-z_]\w*)', expr, re.I)
        if match:
            return {'table': match.group(1), 'column': match.group(2)}

        # 仅字段名
        match = re.search(r'([a-z_]\w*)', expr, re.I)
        if match:
            return {'table': None, 'column': match.group(1)}

        return {'table': None, 'column': None}

    def _identify_indicators(self):
        """识别计算指标"""
        for col_lineage in self.result.column_lineage:
            # 只有聚合字段或派生字段才算指标
            if col_lineage.transform_type in ('DIRECT',):
                continue

            # 推断标准类型
            standard_type = self._infer_standard_type(col_lineage.target_column)

            # 推断指标类别
            if col_lineage.transform_type in ('SUM', 'COUNT', 'AVG', 'MAX', 'MIN'):
                indicator_category = '原子指标'
            elif col_lineage.transform_type in ('CASE', 'CUSTOM'):
                indicator_category = '派生指标'
            else:
                indicator_category = '复合指标'

            # 生成中文名（需用户确认）
            indicator_name = self._generate_chinese_name(col_lineage.target_column)

            # 推断业务域
            business_domain = self._infer_business_domain(self.result.target_table)

            indicator = Indicator(
                indicator_english_name=col_lineage.target_column,
                indicator_name=indicator_name,
                calculation_logic=col_lineage.transform_expr or col_lineage.target_column,
                standard_type=standard_type,
                indicator_category=indicator_category,
                business_domain=business_domain,
                data_type=self._infer_data_type(standard_type),
                update_frequency='日'  # 默认日更新，可根据分区字段调整
            )
            self.result.indicators.append(indicator)

    def _infer_standard_type(self, column_name: str) -> str:
        """推断标准类型"""
        column_lower = column_name.lower()
        for pattern, type_name in self.TYPE_INFERENCE.items():
            if re.search(pattern, column_lower):
                return type_name
        return '数量'  # 默认

    def _infer_business_domain(self, table_name: str) -> str:
        """推断业务域"""
        table_lower = table_name.lower()
        for pattern, domain_name in self.DOMAIN_INFERENCE.items():
            if re.search(pattern, table_lower):
                return domain_name
        return '通用'

    @staticmethod
    def _generate_chinese_name(english_name: str) -> str:
        """生成中文名（占位，需用户确认）"""
        # 简单映射规则（实际需要词根表）
        mapping = {
            'td': '当日',
            'loan': '放款',
            'amt': '金额',
            'cnt': '笔数',
            'avg': '平均',
            'sum': '总',
            'max': '最大',
            'min': '最小',
        }

        parts = re.findall(r'[a-z]+', english_name.lower())
        chinese_parts = [mapping.get(part, f'[{part}]') for part in parts]
        return ''.join(chinese_parts)

    @staticmethod
    def _infer_data_type(standard_type: str) -> str:
        """推断数据类型"""
        if standard_type in ('金额', '平均值'):
            return 'DECIMAL(20,2)'
        elif standard_type in ('数量', '比率'):
            return 'BIGINT'
        else:
            return 'VARCHAR(255)'

    def _generate_summary(self):
        """生成 ETL 逻辑摘要"""
        source_count = len(self.result.table_lineage)
        indicator_count = len(self.result.indicators)

        if source_count == 1:
            summary = f"从 {self.result.table_lineage[0].source_table} 单表加工"
        else:
            summary = f"关联 {source_count} 张源表加工"

        if indicator_count > 0:
            summary += f"，计算 {indicator_count} 个聚合指标"

        self.result.etl_logic_summary = summary


def parse_sql_file(file_path: str) -> SQLAnalysisResult:
    """解析单个 SQL 文件"""
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    parser = SQLParser(sql_content, file_path)
    return parser.parse()


def batch_parse_sql_files(file_paths: List[str]) -> List[SQLAnalysisResult]:
    """批量解析 SQL 文件"""
    results = []
    for file_path in file_paths:
        try:
            result = parse_sql_file(file_path)
            results.append(result)
        except Exception as e:
            print(f"❌ 解析失败: {file_path} - {e}")
    return results


if __name__ == '__main__':
    # 测试用例
    test_sql = """
    INSERT OVERWRITE TABLE dm.dmm_sac_loan_prod_daily PARTITION (dt='${hivevar:dt}')
    SELECT
        a.dt,
        a.product_id,
        b.product_name,
        SUM(a.loan_amount) AS td_loan_amt,
        COUNT(DISTINCT a.loan_id) AS td_loan_cnt,
        SUM(CASE WHEN a.loan_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_cnt,
        SUM(a.loan_amount) / NULLIF(COUNT(DISTINCT a.loan_id), 0) AS avg_loan_amt
    FROM dwd.dwd_loan_detail a
    LEFT JOIN dwd.dwd_product_info b ON a.product_id = b.product_id
    WHERE a.dt = '${hivevar:dt}'
    GROUP BY a.dt, a.product_id, b.product_name
    """

    parser = SQLParser(test_sql, 'test.sql')
    result = parser.parse()

    print(f"目标表: {result.target_table}")
    print(f"\n表级血缘:")
    for tl in result.table_lineage:
        print(f"  - {tl.source_table} ({tl.join_type})")

    print(f"\n字段级血缘:")
    for cl in result.column_lineage:
        print(f"  - {cl.target_column} ← {cl.source_table}.{cl.source_column} [{cl.transform_type}]")

    print(f"\n识别的指标:")
    for ind in result.indicators:
        print(f"  - {ind.indicator_english_name} ({ind.indicator_name}) - {ind.standard_type}")

    print(f"\nETL 逻辑摘要: {result.etl_logic_summary}")
