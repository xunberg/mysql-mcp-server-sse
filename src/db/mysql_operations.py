import os
import logging
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

logger = logging.getLogger("mysql_server")

def get_db_config():
    """动态获取数据库配置"""
    return {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': os.getenv('MYSQL_DATABASE', ''),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'connection_timeout': 5
    }

@contextmanager
def get_db_connection():
    """
    创建数据库连接的上下文管理器
    
    Yields:
        mysql.connector.connection.MySQLConnection: 数据库连接对象
    """
    connection = None
    try:
        db_config = get_db_config()
        if not db_config['database']:
            raise ValueError("数据库名称未设置，请检查环境变量MYSQL_DATABASE")
        
        connection = mysql.connector.connect(**db_config)
        yield connection
    except mysql.connector.Error as err:
        error_msg = str(err)
        logger.error(f"数据库连接失败: {error_msg}")
        
        if "Access denied" in error_msg:
            raise ValueError("访问被拒绝，请检查用户名和密码")
        elif "Unknown database" in error_msg:
            db_config = get_db_config()
            raise ValueError(f"数据库'{db_config['database']}'不存在")
        elif "Can't connect" in error_msg or "Connection refused" in error_msg:
            raise ConnectionError("无法连接到MySQL服务器，请检查服务是否启动")
        elif "Authentication plugin" in error_msg:
            raise ValueError(f"认证插件问题: {error_msg}，请尝试修改用户认证方式为mysql_native_password")
        else:
            raise ConnectionError(f"数据库连接失败: {error_msg}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.debug("数据库连接已关闭")

def execute_query(connection, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    在给定的数据库连接上执行查询
    
    Args:
        connection: 数据库连接
        query: SQL查询语句
        params: 查询参数 (可选)
        
    Returns:
        查询结果列表
    """
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 执行查询
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # 获取结果
        results = cursor.fetchall()
        logger.debug(f"查询返回 {len(results)} 条结果")
        return results
            
    except mysql.connector.Error as query_err:
        logger.error(f"查询执行失败: {str(query_err)}")
        raise ValueError(f"查询执行失败: {str(query_err)}")
    finally:
        # 确保游标正确关闭
        if cursor:
            cursor.close()
            logger.debug("数据库游标已关闭")