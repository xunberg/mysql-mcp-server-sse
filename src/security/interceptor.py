import logging
from typing import List, Dict

from ..config import SecurityConfig, SQLConfig
from .sql_analyzer import SQLOperationType, SQLRiskLevel
from .sql_parser import SQLParser

logger = logging.getLogger(__name__)

class SecurityException(Exception):
    """安全相关异常"""
    pass

class SQLInterceptor:
    """SQL操作拦截器"""
    
    def __init__(self, analyzer: SQLOperationType):
        self.analyzer = analyzer
        # 设置最大SQL长度限制
        self.max_sql_length = SecurityConfig.MAX_SQL_LENGTH

    async def check_operation(self, sql_query: str) -> bool:
        """
        检查SQL操作是否允许执行
        
        Args:
            sql_query: SQL查询语句
            
        Returns:
            bool: 是否允许执行
            
        Raises:
            SecurityException: 当操作被拒绝时抛出
        """
        try:
            # 检查SQL是否为空
            if not sql_query or not sql_query.strip():
                raise SecurityException("SQL语句不能为空")
                
            # 检查SQL长度
            if len(sql_query) > self.max_sql_length:
                raise SecurityException(f"SQL语句长度({len(sql_query)})超出限制({self.max_sql_length})")
                
            # 使用SQLParser解析SQL
            parsed_sql = SQLParser.parse_query(sql_query)
            
            # 检查SQL是否有效
            if not parsed_sql['is_valid']:
                raise SecurityException("SQL语句格式无效")
                
            operation = parsed_sql['operation_type']
            # 更新支持的操作类型列表，包括元数据操作
            supported_operations = SQLConfig.DDL_OPERATIONS | SQLConfig.DML_OPERATIONS | SQLConfig.METADATA_OPERATIONS
                
            if operation not in supported_operations:
                raise SecurityException(f"不支持的SQL操作: {operation}")
            
            # 分析SQL风险
            risk_analysis = self.analyzer.analyze_risk(sql_query)
            
            # 检查是否是危险操作
            if risk_analysis['is_dangerous']:
                raise SecurityException(
                    f"检测到危险操作: {risk_analysis['operation']}"
                )
            
            # 检查操作是否被允许
            if not risk_analysis['is_allowed']:
                raise SecurityException(
                    f"当前操作风险等级({risk_analysis['risk_level'].name})不被允许执行，"
                    f"允许的风险等级: {[level.name for level in self.analyzer.allowed_risk_levels]}"
                )
            
            # 确定操作类型（DDL, DML 或 元数据）
            operation_category = parsed_sql['category']
            
            # 记录详细日志
            logger.info(
                f"SQL{operation_category}操作检查通过 - "
                f"操作: {risk_analysis['operation']}, "
                f"风险等级: {risk_analysis['risk_level'].name}, "
                f"影响表: {', '.join(risk_analysis['affected_tables'])}"
            )

            return True

        except SecurityException as e:
            logger.error(str(e))
            raise
        except Exception as e:
            error_msg = f"安全检查失败: {str(e)}"
            logger.error(error_msg)
            raise SecurityException(error_msg) 