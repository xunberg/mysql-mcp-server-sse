# MySQL查询服务器

这是一个基于MCP（Model-Controller-Provider）框架的MySQL查询服务器，提供了通过SSE（Server-Sent Events）进行MySQL数据库操作的功能。

## 功能特点

- 基于FastMCP框架构建
- 支持SSE（Server-Sent Events）实时数据传输
- 提供MySQL数据库查询接口
- 完整的日志记录系统
- 自动事务管理（提交/回滚）
- 环境变量配置支持
- SQL安全检查机制
  - 风险等级控制
  - SQL注入防护
  - 危险操作拦截
  - WHERE子句强制检查
  - 自动返回修改操作影响的行数
- 敏感信息保护机制
- 自动对元数据查询结果进行格式化和增强

## API接口功能

系统提供以下四大类工具：

### 基础查询工具

- `mysql_query`: 执行任意SQL查询，支持参数化查询

### 元数据查询工具

- `mysql_show_tables`: 获取数据库中的表列表，支持模式匹配和限制结果数量
- `mysql_show_columns`: 获取表的列信息
- `mysql_describe_table`: 描述表结构
- `mysql_show_create_table`: 获取表的创建语句

### 数据库信息查询工具

- `mysql_show_databases`: 获取所有数据库列表，支持过滤系统数据库
- `mysql_show_variables`: 获取MySQL服务器变量
- `mysql_show_status`: 获取MySQL服务器状态信息

### 表结构高级查询工具

- `mysql_show_indexes`: 获取表的索引信息
- `mysql_show_table_status`: 获取表状态信息
- `mysql_show_foreign_keys`: 获取表的外键约束信息
- `mysql_paginate_results`: 提供结果分页功能

## 系统要求

- Python 3.6+
- MySQL服务器
- 依赖包：
  - mysql-connector-python
  - python-dotenv
  - mcp (FastMCP框架)

## 安装步骤

1. 克隆项目到本地：
```bash
git clone [项目地址]
cd mysql-query-server
```

2. 安装依赖包：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
   - 复制`.env.example`文件并重命名为`.env`
   - 根据实际情况修改`.env`文件中的配置

## 环境变量配置

在`.env`文件中配置以下参数：

### 基本配置
- `HOST`: 服务器监听地址（默认：127.0.0.1）
- `PORT`: 服务器监听端口（默认：3000）
- `MYSQL_HOST`: MySQL服务器地址
- `MYSQL_PORT`: MySQL服务器端口
- `MYSQL_USER`: MySQL用户名
- `MYSQL_PASSWORD`: MySQL密码
- `MYSQL_DATABASE`: MySQL数据库名

### SQL安全配置
- `ENV_TYPE`: 环境类型（development/production）
- `ALLOWED_RISK_LEVELS`: 允许的风险等级（LOW/MEDIUM/HIGH/CRITICAL）
- `BLOCKED_PATTERNS`: 禁止的SQL模式（正则表达式，用逗号分隔）
- `ENABLE_QUERY_CHECK`: 是否启用SQL安全检查（true/false）
- `ALLOW_SENSITIVE_INFO`: 是否允许查询敏感信息（true/false）
- `SENSITIVE_INFO_FIELDS`: 自定义敏感字段模式列表（逗号分隔）

## 安全机制详解

### 风险等级控制
- LOW: 查询操作（SELECT）和元数据操作（SHOW, DESCRIBE等）
- MEDIUM: 基本数据修改（INSERT，有WHERE的UPDATE/DELETE）
- HIGH: 结构变更（CREATE/ALTER）和无WHERE的UPDATE
- CRITICAL: 危险操作（DROP/TRUNCATE）和无WHERE的DELETE操作

### 环境特性差异
- 开发环境：
  - 允许较高风险的操作
  - 不隐藏敏感信息
  - 提供详细的错误信息
- 生产环境：
  - 默认只允许LOW风险操作
  - 严格限制数据修改
  - 自动隐藏敏感信息
  - 错误信息不暴露实现细节

### 敏感信息保护
系统会自动检测并隐藏包含以下关键词的变量/状态值：
- password、auth、credential、key、secret、private
- ssl、tls、cipher、certificate
- host、path、directory等系统路径信息

### 事务管理
- 对于修改操作（INSERT/UPDATE/DELETE）会自动提交事务
- 执行错误时自动回滚事务
- 返回操作影响的行数

## 启动服务器

运行以下命令启动服务器：

```bash
python src/server.py
```

服务器将在配置的地址和端口上启动，默认为 `http://127.0.0.1:3000/sse`

## 项目结构

```
.
├── src/                     # 源代码目录
│   ├── server.py           # 主服务器文件
│   ├── db/                 # 数据库相关代码
│   │   └── mysql_operations.py # MySQL操作实现
│   ├── security/           # SQL安全相关代码
│   │   ├── interceptor.py   # SQL拦截器
│   │   ├── query_limiter.py # SQL安全检查器
│   │   └── sql_analyzer.py  # SQL分析器
│   └── tools/              # 工具类代码
│       ├── mysql_tool.py           # 基础查询工具
│       ├── mysql_metadata_tool.py  # 元数据查询工具
│       ├── mysql_info_tool.py      # 数据库信息查询工具
│       ├── mysql_schema_tool.py    # 表结构高级查询工具
│       └── metadata_base_tool.py   # 元数据工具基类
├── tests/                  # 测试代码目录
├── .env.example            # 环境变量示例文件
└── requirements.txt        # 项目依赖文件
```

## 常见问题解决

### DELETE操作未执行成功
- 检查DELETE操作是否包含WHERE条件
- 无WHERE条件的DELETE操作被标记为CRITICAL风险级别
- 确保环境变量ALLOWED_RISK_LEVELS中包含CRITICAL（如果需要执行该操作）
- 检查影响行数返回值，确认操作是否实际影响了数据库

### 环境变量未生效
- 确保在server.py中的load_dotenv()调用发生在导入其他模块之前
- 重启应用以确保环境变量被正确加载
- 检查日志中"从环境变量读取到的风险等级设置"的输出

### 操作被安全机制拒绝
- 检查操作的风险级别是否在允许的范围内
- 如果需要执行高风险操作，相应地调整ALLOWED_RISK_LEVELS
- 对于不带WHERE条件的UPDATE或DELETE，可以添加条件（即使是WHERE 1=1）降低风险级别

### 无法查看敏感信息
- 在开发环境中，设置ALLOW_SENSITIVE_INFO=true
- 在生产环境中，敏感信息默认会被隐藏，这是安全特性

## 日志系统

服务器包含完整的日志记录系统，可以在控制台和日志文件中查看运行状态和错误信息。日志级别可以在`server.py`中配置。

## 错误处理

服务器包含完善的错误处理机制：
- MySQL连接器导入检查
- 数据库配置验证
- SQL安全检查
- 运行时错误捕获和记录
- 事务自动回滚

## 贡献指南

欢迎提交Issue和Pull Request来改进项目。

## 许可证

MIT License

Copyright (c) 2024 MCP MySQL Query Server

特此免费授予任何获得本软件副本和相关文档文件（"软件"）的人不受限制地处理本软件的权利，包括不受限制地使用、复制、修改、合并、发布、分发、再许可和/或出售本软件副本，以及允许本软件的使用者这样做，但须符合以下条件：

上述版权声明和本许可声明应包含在本软件的所有副本或重要部分中。

本软件按"原样"提供，不提供任何形式的明示或暗示的保证，包括但不限于对适销性、特定用途的适用性和非侵权性的保证。在任何情况下，作者或版权持有人均不对任何索赔、损害或其他责任负责，无论是在合同诉讼、侵权行为还是其他方面，产生于、源于或与本软件有关，或与本软件的使用或其他交易有关。