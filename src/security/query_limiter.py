import os
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class QueryLimiter:
    """查询安全检查器"""
    
    def __init__(self):
        # 解析启用状态（默认启用）
        enable_check = os.getenv('ENABLE_QUERY_CHECK', 'true')
        self.enable_check = str(enable_check).lower() not in {'false', '0', 'no', 'off'}

    def check_query(self, sql_query: str) -> Tuple[bool, str]:
        """
        检查SQL查询是否安全
        
        Args:
            sql_query: SQL查询语句
            
        Returns:
            Tuple[bool, str]: (是否允许执行, 错误信息)
        """
        if not self.enable_check:
            return True, ""
            
        sql_query = sql_query.strip().upper()
        operation_type = self._get_operation_type(sql_query)
        
        # 检查是否为无 WHERE 子句的更新/删除操作
        if operation_type in {'UPDATE', 'DELETE'} and 'WHERE' not in sql_query:
            error_msg = f"{operation_type}操作必须包含WHERE子句"
            logger.warning(f"查询被限制: {error_msg}")
            return False, error_msg
            
        return True, ""

    def _get_operation_type(self, sql_query: str) -> str:
        """获取SQL操作类型"""
        if not sql_query:
            return ""
        words = sql_query.split()
        if not words:
            return ""
        return words[0].upper()

    def _parse_int_env(self, env_name: str, default: int) -> int:
        """解析整数类型的环境变量"""
        try:
            return int(os.getenv(env_name, str(default)))
        except (ValueError, TypeError):
            return default

    def update_limits(self, new_limits: dict):
        """
        更新限制阈值
        
        Args:
            new_limits: 新的限制值字典
        """
        for operation, limit in new_limits.items():
            if operation in self.max_limits:
                try:
                    self.max_limits[operation] = int(limit)
                    logger.info(f"更新{operation}操作的限制为: {limit}")
                except (ValueError, TypeError):
                    logger.warning(f"无效的限制值: {operation}={limit}") 