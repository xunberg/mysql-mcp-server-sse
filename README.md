# MySQL查询服务器

这是一个基于MCP（Model-Controller-Provider）框架的MySQL查询服务器，提供了通过SSE（Server-Sent Events）进行MySQL数据库操作的功能。

## 功能特点

- 基于FastMCP框架构建
- 支持SSE（Server-Sent Events）实时数据传输
- 提供MySQL数据库查询接口
- 完整的日志记录系统
- 环境变量配置支持

## 系统要求

- Python 3.6+
- MySQL服务器

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

- `HOST`: 服务器监听地址（默认：127.0.0.1）
- `PORT`: 服务器监听端口（默认：3000）
- `MYSQL_HOST`: MySQL服务器地址
- `MYSQL_PORT`: MySQL服务器端口
- `MYSQL_USER`: MySQL用户名
- `MYSQL_PASSWORD`: MySQL密码
- `MYSQL_DATABASE`: MySQL数据库名

## 启动服务器

运行以下命令启动服务器：

```bash
python src/server.py
```

服务器将在配置的地址和端口上启动，默认为 `http://127.0.0.1:3000/sse`

## 项目结构

```
.
├── src/                    # 源代码目录
│   ├── server.py          # 主服务器文件
│   ├── db/                # 数据库相关代码
│   └── tools/             # 工具类代码
├── .env.example           # 环境变量示例文件
└── requirements.txt       # 项目依赖文件
```

## 日志系统

服务器包含完整的日志记录系统，可以在控制台和日志文件中查看运行状态和错误信息。日志级别可以在`server.py`中配置。

## 错误处理

服务器包含完善的错误处理机制：
- MySQL连接器导入检查
- 数据库配置验证
- 运行时错误捕获和记录

## 贡献指南

欢迎提交Issue和Pull Request来改进项目。

## 许可证

MIT License

Copyright (c) 2024 MCP MySQL Query Server

特此免费授予任何获得本软件副本和相关文档文件（"软件"）的人不受限制地处理本软件的权利，包括不受限制地使用、复制、修改、合并、发布、分发、再许可和/或出售本软件副本，以及允许本软件的使用者这样做，但须符合以下条件：

上述版权声明和本许可声明应包含在本软件的所有副本或重要部分中。

本软件按"原样"提供，不提供任何形式的明示或暗示的保证，包括但不限于对适销性、特定用途的适用性和非侵权性的保证。在任何情况下，作者或版权持有人均不对任何索赔、损害或其他责任负责，无论是在合同诉讼、侵权行为还是其他方面，产生于、源于或与本软件有关，或与本软件的使用或其他交易有关。