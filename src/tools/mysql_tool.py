import json
import logging
from typing import Any, Dict, Optional
from mcp.server.fastmcp import FastMCP
from src.db.mysql_operations import get_db_connection, execute_query
import mysql.connector

logger = logging.getLogger("mysql_server")

# 尝试导入MySQL连接器
try:
    mysql.connector
    mysql_available = True
except ImportError:
    mysql_available = False

def register_mysql_tool(mcp: FastMCP):
    """
    注册MySQL查询工具到MCP服务器
    
    Args:
        mcp: FastMCP服务器实例
    """
    logger.debug("注册MySQL查询工具...")
    
    @mcp.tool()
    def mysql_query(query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        执行MySQL查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数 (可选)
            
        Returns:
            查询结果的JSON字符串
        """
        logger.debug(f"执行MySQL查询: {query}, 参数: {params}")
        
        try:
            with get_db_connection() as connection:
                results = execute_query(connection, query, params)
                return json.dumps(results, default=str)
                
        except Exception as e:
            logger.error(f"执行查询时发生异常: {str(e)}")
            return json.dumps({"error": str(e)})