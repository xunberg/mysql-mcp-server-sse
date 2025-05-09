import json
import logging
from typing import Any, Dict, Optional
from mcp.server.fastmcp import FastMCP
from src.db.mysql_operations import get_db_connection, execute_query
import aiomysql

from .metadata_base_tool import MetadataToolBase

logger = logging.getLogger("mysql_server")

# MySQL可用性检查变量，默认认为aiomysql已可用
mysql_available = True

def register_mysql_tool(mcp: FastMCP):
    """
    注册MySQL查询工具到MCP服务器
    
    Args:
        mcp: FastMCP服务器实例
    """
    logger.debug("注册MySQL查询工具...")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_query(query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        执行MySQL查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数 (可选)
            
        Returns:
            查询结果的JSON字符串
        """
        logger.debug(f"执行MySQL查询: {query}, 参数: {params}")
        
        async with get_db_connection() as connection:
            results = await execute_query(connection, query, params)
            
            # 检查是否是修改操作返回的影响行数
            operation = query.strip().split()[0].upper()
            if operation in {'UPDATE', 'DELETE', 'INSERT'} and results and 'affected_rows' in results[0]:
                affected_rows = results[0]['affected_rows']
                logger.info(f"{operation}操作影响了{affected_rows}行数据")
                
            # 添加元数据信息
            metadata_info = {
                "metadata_info": {
                    "operation_type": operation,
                    "result_count": len(results)
                },
                "results": results
            }
            
            return json.dumps(metadata_info, default=str)