# MySQL-MCP

基于MCP协议的MySQL查询工具，允许通过MCP接口执行SQL查询并返回结果。

## 功能特点

- 支持连接到MySQL数据库并执行SQL查询
- 支持查询类SQL语句（SELECT, SHOW, DESCRIBE, EXPLAIN）
- 支持非查询类SQL语句（INSERT, UPDATE, DELETE等）
- 查询结果以JSON格式返回
- 通过环境变量配置数据库连接参数
- 使用连接池管理数据库连接

## 安装

### 前置条件

- Go 1.23.3 或更高版本
- MySQL 数据库服务器

### 安装步骤

1. 克隆仓库

```bash
git clone https://github.com/yourusername/mysql-mcp.git
cd mysql-mcp
```

2. 安装依赖

```bash
go mod download
```

3. 编译项目

```bash
go build -o mysql-mcp
```

## 配置

在项目根目录创建`.env`文件，配置以下环境变量：

```
DB_USER=your_mysql_username
DB_PASSWORD=your_mysql_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database_name
DB_PARAMS=parseTime=true&charset=utf8mb4&loc=Local
```

参数说明：
- `DB_USER`: MySQL用户名
- `DB_PASSWORD`: MySQL密码
- `DB_HOST`: MySQL服务器地址
- `DB_PORT`: MySQL服务器端口
- `DB_NAME`: 数据库名称
- `DB_PARAMS`: 额外的连接参数（可选）

## 使用方法

### 启动服务

```bash
./mysql-mcp
```

启动后，服务将通过标准输入/输出与MCP客户端通信。

### MCP Host端配置

在MCP Host端的配置文件中，可以按照以下方式配置mysql-mcp服务：

```json
"mcpMysql": {
  "command": "<mysql-mcp安装路径>/mysql-mcp",
  "args":["-m", "query", "--query=SELECT NOW()"]
}
```

配置说明：
- `command`: mysql-mcp可执行文件的路径，应指向编译后的可执行文件位置
- `args`: 命令行参数（本工具不需要额外参数，通过MCP协议直接调用`execute_sql`工具）

注意：
- 确保mysql-mcp可执行文件具有执行权限
- 确保.env文件已正确配置并位于可执行文件同目录下
- 工具会自动加载环境变量并建立数据库连接

### 执行SQL查询

通过MCP协议，可以调用`execute_sql`工具执行SQL查询：

```json
{
  "tool": "execute_sql",
  "params": {
    "query": "SELECT * FROM users LIMIT 10"
  }
}
```

### 查询结果示例

对于查询类SQL语句，结果将以JSON格式返回：

```json
[
  {
    "id": 1,
    "name": "张三",
    "email": "zhangsan@example.com",
    "created_at": "2023-01-01 12:00:00"
  },
  {
    "id": 2,
    "name": "李四",
    "email": "lisi@example.com",
    "created_at": "2023-01-02 13:30:00"
  }
]
```

对于非查询类SQL语句，将返回执行结果信息：

```
Query executed successfully. Rows affected: 1, Last insert ID: 3
```


## 注意事项

- 请确保数据库连接参数正确配置
- 生产环境中应注意SQL注入安全问题
- 建议限制查询结果集大小，避免内存占用过高