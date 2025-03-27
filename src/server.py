from mcp.server.fastmcp import FastMCP
import os
import logging
from dotenv import load_dotenv

# 加载环境变量 - 移到最前面确保所有模块导入前环境变量已加载
load_dotenv()

# 导入自定义模块 - 确保在load_dotenv之后导入
from src.tools.mysql_tool import register_mysql_tool

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mysql_server")

# 记录环境变量加载情况
logger.debug("已加载环境变量")
logger.debug(f"当前允许的风险等级: {os.getenv('ALLOWED_RISK_LEVELS', '未设置')}")

# 尝试导入MySQL连接器
try:
    import mysql.connector
    logger.debug("MySQL连接器导入成功")
    mysql_available = True
except ImportError as e:
    logger.critical(f"无法导入MySQL连接器: {str(e)}")
    logger.critical("请确保已安装mysql-connector-python包: pip install mysql-connector-python")
    mysql_available = False

# 从环境变量获取服务器配置
host = os.getenv('HOST', '127.0.0.1')
port = int(os.getenv('PORT', '3000'))
logger.debug(f"服务器配置: host={host}, port={port}")

# 创建MCP服务器实例
logger.debug("正在创建MCP服务器实例...")
mcp = FastMCP("MySQL Query Server", "cccccccccc", host=host, port=port, debug=True, endpoint='/sse')
logger.debug("MCP服务器实例创建完成")

# 注册MySQL工具
register_mysql_tool(mcp)

def start_server():
    """启动SSE服务器的同步包装器"""
    logger.debug("开始启动MySQL查询服务器...")
    
    print(f"开始启动MySQL查询SSE服务器...")
    print(f"服务器监听在 {host}:{port}/sse")
    
    try:
        # 检查MySQL配置是否有效
        from src.db.mysql_operations import get_db_config
        db_config = get_db_config()
        if mysql_available and not db_config['database']:
            logger.warning("未设置数据库名称，请检查环境变量MYSQL_DATABASE")
            print("警告: 未设置数据库名称，请检查环境变量MYSQL_DATABASE")
        
        # 使用run_app函数启动服务器
        logger.debug("调用mcp.run('sse')启动服务器...")
        mcp.run('sse')
    except Exception as e:
        logger.exception(f"服务器运行时发生错误: {str(e)}")
        print(f"服务器运行时发生错误: {str(e)}")
    
if __name__ == "__main__":
    # 确保初始化后工具才被注册
    start_server()
