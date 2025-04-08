"""
MySQL元数据查询工具
提供表结构等元数据信息查询功能
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP

from .metadata_base_tool import MetadataToolBase, ParameterValidationError, QueryExecutionError
from src.db.mysql_operations import get_db_connection, execute_query

logger = logging.getLogger("mysql_server")

# 工具函数: 用于参数验证
def validate_pattern(pattern: str) -> bool:
    """
    验证模式字符串是否安全 (防止SQL注入)
    
    Args:
        pattern: 要验证的模式字符串
        
    Returns:
        如果模式安全返回True，否则抛出ValueError
    
    Raises:
        ValueError: 当模式包含不安全字符时
    """
    # 仅允许字母、数字、下划线和通配符(% 和 _)
    if not re.match(r'^[a-zA-Z0-9_%]+$', pattern):
        raise ValueError("模式只能包含字母、数字、下划线和通配符(%_)")
    return True

def validate_table_name(name: str) -> bool:
    """
    验证表名是否合法安全
    
    Args:
        name: 要验证的表名
        
    Returns:
        如果表名安全返回True，否则抛出ValueError
    
    Raises:
        ValueError: 当表名包含不安全字符时
    """
    # 仅允许字母、数字和下划线
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise ValueError(f"无效的表名: {name}, 表名只能包含字母、数字和下划线")
    return True

def validate_database_name(name: str) -> bool:
    """
    验证数据库名是否合法安全
    
    Args:
        name: 要验证的数据库名
        
    Returns:
        如果数据库名安全返回True，否则抛出ValueError
    
    Raises:
        ValueError: 当数据库名包含不安全字符时
    """
    # 仅允许字母、数字和下划线
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise ValueError(f"无效的数据库名: {name}, 数据库名只能包含字母、数字和下划线")
    return True

def register_metadata_tools(mcp: FastMCP):
    """
    注册MySQL元数据查询工具到MCP服务器
    
    Args:
        mcp: FastMCP服务器实例
    """
    logger.debug("注册MySQL元数据查询工具...")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_tables(database: Optional[str] = None, pattern: Optional[str] = None,
                               limit: int = 100, exclude_views: bool = False) -> str:
        """
        获取数据库中的表列表，支持筛选和限制结果数量
        
        Args:
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            pattern: 表名匹配模式 (可选, 例如 '%user%')
            limit: 返回结果的最大数量 (默认100，设为0表示无限制)
            exclude_views: 是否排除视图 (默认为False)
            
        Returns:
            表列表的JSON字符串
        """
        # 参数验证
        if database:
            MetadataToolBase.validate_parameter(
                "database", database, 
                lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
                "数据库名称只能包含字母、数字和下划线"
            )
            
        if pattern:
            MetadataToolBase.validate_parameter(
                "pattern", pattern,
                lambda x: re.match(r'^[a-zA-Z0-9_%]+$', x),
                "模式只能包含字母、数字、下划线和通配符(%_)"
            )
            
        MetadataToolBase.validate_parameter(
            "limit", limit,
            lambda x: isinstance(x, int) and x >= 0,
            "返回结果的最大数量必须是非负整数"
        )
        
        # 基础查询
        base_query = "SHOW FULL TABLES" if exclude_views else "SHOW TABLES"
        if database:
            base_query += f" FROM `{database}`"
        if pattern:
            base_query += f" LIKE '{pattern}'"
            
        logger.debug(f"执行查询: {base_query}")
        
        # 执行查询
        with get_db_connection() as connection:
            results = await execute_query(connection, base_query)
            
            # 如果需要排除视图，且使用的是SHOW FULL TABLES
            if exclude_views and "FULL" in base_query:
                filtered_results = []
                
                # 查找表名和表类型字段
                fields = list(results[0].keys()) if results else []
                table_field = fields[0] if fields else None
                table_type_field = fields[1] if len(fields) > 1 else None
                
                if table_field and table_type_field:
                    # 基表类型通常是"BASE TABLE"
                    for item in results:
                        if item[table_type_field] == 'BASE TABLE':
                            filtered_results.append(item)
                else:
                    filtered_results = results
            else:
                filtered_results = results
                
            # 限制返回数量
            if limit > 0 and len(filtered_results) > limit:
                limited_results = filtered_results[:limit]
                is_limited = True
            else:
                limited_results = filtered_results
                is_limited = False
                
            # 构造元数据
            metadata_info = {
                "metadata_info": {
                    "operation_type": "表列表查询",
                    "result_count": len(limited_results),
                    "total_count": len(results),
                    "filtered": {
                        "database": database,
                        "pattern": pattern,
                        "exclude_views": exclude_views and "FULL" in base_query,
                        "limited": is_limited
                    }
                },
                "results": limited_results
            }
            
            return json.dumps(metadata_info, default=str)
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_columns(table: str, database: Optional[str] = None) -> str:
        """
        获取表的列信息
        
        Args:
            table: 表名
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            
        Returns:
            表列信息的JSON字符串
        """
        # 参数验证
        MetadataToolBase.validate_parameter(
            "table", table,
            lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
            "表名只能包含字母、数字和下划线"
        )
        
        if database:
            MetadataToolBase.validate_parameter(
                "database", database, 
                lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
                "数据库名称只能包含字母、数字和下划线"
            )
            
        query = f"SHOW COLUMNS FROM `{table}`" if not database else f"SHOW COLUMNS FROM `{database}`.`{table}`"
        logger.debug(f"执行查询: {query}")
        
        return await MetadataToolBase.execute_metadata_query(query, operation_type="表列信息查询")

    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_describe_table(table: str, database: Optional[str] = None) -> str:
        """
        描述表结构
        
        Args:
            table: 表名
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            
        Returns:
            表结构描述的JSON字符串
        """
        # 参数验证
        MetadataToolBase.validate_parameter(
            "table", table,
            lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
            "表名只能包含字母、数字和下划线"
        )
        
        if database:
            MetadataToolBase.validate_parameter(
                "database", database, 
                lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
                "数据库名称只能包含字母、数字和下划线"
            )
            
        # DESCRIBE 语句与 SHOW COLUMNS 功能类似，但结果格式可能略有不同
        query = f"DESCRIBE `{table}`" if not database else f"DESCRIBE `{database}`.`{table}`"
        logger.debug(f"执行查询: {query}")
        
        return await MetadataToolBase.execute_metadata_query(query, operation_type="表结构描述")

    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_create_table(table: str, database: Optional[str] = None) -> str:
        """
        获取表的创建语句
        
        Args:
            table: 表名
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            
        Returns:
            表创建语句的JSON字符串
        """
        # 参数验证
        MetadataToolBase.validate_parameter(
            "table", table,
            lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
            "表名只能包含字母、数字和下划线"
        )
        
        if database:
            MetadataToolBase.validate_parameter(
                "database", database, 
                lambda x: re.match(r'^[a-zA-Z0-9_]+$', x),
                "数据库名称只能包含字母、数字和下划线"
            )
            
        table_ref = f"`{table}`" if not database else f"`{database}`.`{table}`"
        query = f"SHOW CREATE TABLE {table_ref}"
        logger.debug(f"执行查询: {query}")
        
        return await MetadataToolBase.execute_metadata_query(query, operation_type="表创建语句查询")