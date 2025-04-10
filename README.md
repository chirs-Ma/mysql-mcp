# MySQL MCP 项目

这是一个基于 MySQL 数据库和 Milvus 向量数据库的 MCP (Model Control Protocol) 项目，用于提供数据库查询和向量相似度搜索功能。

## 环境配置

项目需要以下环境变量配置（在 `.env` 文件中设置）：

### MySQL 数据库配置
- `DB_USER`: 数据库用户名
- `DB_PASSWORD`: 数据库密码
- `DB_HOST`: 数据库主机地址
- `DB_PORT`: 数据库端口（默认 3306）
- `DB_NAME`: 数据库名称
- `DB_PARAMS`: 数据库连接参数（如字符集、时区等）

### SiliconFlow API 配置（用于向量嵌入）
- `SILICONFLOW_TOKEN`: SiliconFlow API 访问令牌
- `SILICONFLOW_URL`: SiliconFlow API 端点 URL

### Milvus 向量数据库配置
- `MILVUS_HOST`: Milvus 服务器地址
- `MILVUS_PORT`: Milvus 服务端口（默认 19530）
- `MILVUS_COLLECTION`: Milvus 集合名称

## 功能特性

- 执行 SQL 查询：通过 `execute_sql` 工具执行 MySQL 数据库查询
- 表结构查询：通过 `get_can_use_table` 工具根据自然语言描述查找相关表结构
- 向量相似度搜索：使用 Milvus 进行高效的向量相似度搜索

## 使用方法

1. 确保已安装 MySQL 和 Milvus 数据库
2. 配置 `.env` 文件中的环境变量
3. 运行应用程序：`go run main.go`

## 依赖项

- Go 1.16+
- MySQL 数据库
- Milvus 向量数据库
- SiliconFlow API（用于生成文本嵌入向量）