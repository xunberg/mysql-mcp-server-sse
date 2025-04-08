"""
MySQL表结构高级查询工具
提供索引、约束、表状态等高级元数据查询功能
"""

import json
import logging
import re
import os
from typing import Any, Dict, List, Optional, Union
from mcp.server.fastmcp import FastMCP

from .metadata_base_tool import MetadataToolBase, ParameterValidationError, QueryExecutionError
from src.db.mysql_operations import get_db_connection, execute_query

logger = logging.getLogger("mysql_server")

# 参数验证函数
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

def validate_column_name(name: str) -> bool:
    """
    验证列名是否合法安全
    
    Args:
        name: 要验证的列名
        
    Returns:
        如果列名安全返回True，否则抛出ValueError
    
    Raises:
        ValueError: 当列名包含不安全字符时
    """
    # 仅允许字母、数字和下划线
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise ValueError(f"无效的列名: {name}, 列名只能包含字母、数字和下划线")
    return True

async def execute_schema_query(
    query: str, 
    params: Optional[Dict[str, Any]] = None, 
    operation_type: str = "元数据查询"
) -> str:
    """
    执行表结构查询
    
    Args:
        query: SQL查询语句
        params: 查询参数 (可选)
        operation_type: 操作类型描述
        
    Returns:
        查询结果的JSON字符串
    """
    with get_db_connection() as connection:
        results = await execute_query(connection, query, params)
        return MetadataToolBase.format_results(results, operation_type)

def register_schema_tools(mcp: FastMCP):
    """
    注册MySQL表结构高级查询工具到MCP服务器
    
    Args:
        mcp: FastMCP服务器实例
    """
    logger.debug("注册MySQL表结构高级查询工具...")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_indexes(table: str, database: Optional[str] = None) -> str:
        """
        获取表的索引信息
        
        Args:
            table: 表名
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            
        Returns:
            表索引信息的JSON字符串
        """
        # 参数验证
        validate_table_name(table)
        
        if database:
            validate_database_name(database)
        
        # 构建查询
        table_ref = f"`{table}`" if not database else f"`{database}`.`{table}`"
        query = f"SHOW INDEX FROM {table_ref}"
        logger.debug(f"执行查询: {query}")
        
        # 执行查询
        return await execute_schema_query(query, operation_type="表索引查询")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_table_status(database: Optional[str] = None, like_pattern: Optional[str] = None) -> str:
        """
        获取表状态信息
        
        Args:
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            like_pattern: 表名匹配模式 (可选，例如 '%user%')
            
        Returns:
            表状态信息的JSON字符串
        """
        # 参数验证
        if database:
            validate_database_name(database)
            
        if like_pattern:
            validate_column_name(like_pattern)
        
        # 构建查询
        if database:
            query = f"SHOW TABLE STATUS FROM `{database}`"
        else:
            query = "SHOW TABLE STATUS"
            
        if like_pattern:
            query += f" LIKE '{like_pattern}'"
            
        logger.debug(f"执行查询: {query}")
        
        # 执行查询
        return await execute_schema_query(query, operation_type="表状态查询")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_show_foreign_keys(table: str, database: Optional[str] = None) -> str:
        """
        获取表的外键约束信息
        
        Args:
            table: 表名
            database: 数据库名称 (可选，默认使用当前连接的数据库)
            
        Returns:
            表外键约束信息的JSON字符串
        """
        # 参数验证
        validate_table_name(table)
        
        if database:
            validate_database_name(database)
        
        # 确定数据库名
        db_name = database
        if not db_name:
            # 获取当前数据库
            with get_db_connection() as connection:
                current_db_results = await execute_query(connection, "SELECT DATABASE() as db")
                if current_db_results and 'db' in current_db_results[0]:
                    db_name = current_db_results[0]['db']
        
        if not db_name:
            raise ValueError("无法确定数据库名称，请明确指定database参数")
        
        # 使用INFORMATION_SCHEMA查询外键
        query = """
        SELECT 
            CONSTRAINT_NAME, 
            TABLE_NAME,
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME,
            UPDATE_RULE,
            DELETE_RULE
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN 
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        ON 
            kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        WHERE 
            kcu.TABLE_SCHEMA = %s 
            AND kcu.TABLE_NAME = %s
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """
        params = {'TABLE_SCHEMA': db_name, 'TABLE_NAME': table}
        
        logger.debug(f"执行查询: 获取表 {db_name}.{table} 的外键约束")
        
        # 执行查询
        return await execute_schema_query(query, params, operation_type="外键约束查询")
    
    @mcp.tool()
    @MetadataToolBase.handle_query_error
    async def mysql_paginate_results(query: str, page: int = 1, page_size: int = 50) -> str:
        """
        分页执行查询以处理大型结果集
        
        Args:
            query: SQL查询语句
            page: 页码 (从1开始)
            page_size: 每页记录数 (默认50)
            
        Returns:
            分页结果的JSON字符串
        """
        # 参数验证
        MetadataToolBase.validate_parameter(
            "page", page,
            lambda x: isinstance(x, int) and x > 0,
            "页码必须是正整数"
        )
        
        MetadataToolBase.validate_parameter(
            "page_size", page_size,
            lambda x: isinstance(x, int) and 1 <= x <= 1000,
            "每页记录数必须在1-1000之间"
        )
        
        # 检查查询语法
        if not query.strip().upper().startswith('SELECT'):
            raise ValueError("只支持SELECT查询的分页")
            
        # 计算LIMIT和OFFSET
        offset = (page - 1) * page_size
        
        # 在查询末尾添加LIMIT子句
        paginated_query = query.strip()
        if 'LIMIT' in paginated_query.upper():
            raise ValueError("查询已包含LIMIT子句，请移除后重试")
            
        paginated_query += f" LIMIT {page_size} OFFSET {offset}"
        
        logger.debug(f"执行分页查询: 页码={page}, 每页记录数={page_size}")
        
        # 获取总记录数（用于计算总页数）
        count_query = f"SELECT COUNT(*) as total FROM ({query}) as temp_count_table"
        
        with get_db_connection() as connection:
            # 执行分页查询
            results = await execute_query(connection, paginated_query)
            
            # 获取总记录数
            count_results = await execute_query(connection, count_query)
            total_records = count_results[0]['total'] if count_results else 0
            
            # 计算总页数
            total_pages = (total_records + page_size - 1) // page_size
            
            # 构建分页元数据
            pagination_info = {
                "metadata_info": {
                    "operation_type": "分页查询",
                    "result_count": len(results),
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_records": total_records,
                        "total_pages": total_pages
                    }
                },
                "results": results
            }
            
            return json.dumps(pagination_info, default=str) 