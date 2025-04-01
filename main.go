package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// 全局数据库连接变量
var db *sql.DB

// 初始化数据库连接
func initDB(dsn string) error {
	var err error
	// 设置连接超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// 测试连接（使用带超时的上下文）
	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping MySQL: %v", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5) // 设置连接最大生命周期
	db.SetConnMaxIdleTime(time.Minute * 2) // 设置空闲连接最大生命周期

	return nil
}

// 从环境变量构建DSN字符串
func buildDSNFromEnv() string {
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbname := os.Getenv("DB_NAME")
	params := os.Getenv("DB_PARAMS")

	// 构建DSN字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)
	if params != "" {
		dsn += "?" + params
	}

	return dsn
}

func main() {
	// 加载.env文件
	err := godotenv.Load(filepath.Join(filepath.Dir(os.Args[0]), ".env"))
	if err != nil {
		log.Fatalf("警告: 无法加载.env文件: %v\n", err)
	}

	// 初始化数据库连接
	dsn := buildDSNFromEnv()
	log.Println("Connecting to MySQL database...")
	if err := initDB(dsn); err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	log.Println("Successfully connected to MySQL database")
	defer db.Close()

	// Create MCP server
	s := server.NewMCPServer(
		"mcp-go",
		"1.0.0",
	)
	// Add tool
	tool := mcp.NewTool("execute_sql",
		mcp.WithDescription("Execute an SQL query on the MySQL server "),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("query"),
		),
	)
	// Add tool handler
	s.AddTool(tool, executeSql)
	// Start the stdio server
	if err := server.ServeStdio(s); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

func executeSql(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := request.Params.Arguments["query"].(string)
	fmt.Printf("Executing query: %s\n", query)
	if query == "" {
		return nil, fmt.Errorf("query is empty")
	}

	// 检查数据库连接是否可用
	if db == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// 判断SQL语句类型（简单判断，实际应用中可能需要更复杂的解析）
	queryLower := strings.ToLower(strings.TrimSpace(query))
	isQuery := strings.HasPrefix(queryLower, "select") || strings.HasPrefix(queryLower, "show") ||
		strings.HasPrefix(queryLower, "describe") || strings.HasPrefix(queryLower, "explain")

	// 如果是查询语句
	if isQuery {
		// 执行查询
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("query execution failed: %v", err)
		}
		defer rows.Close()

		// 获取列名
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get column names: %v", err)
		}

		// 准备结果集
		resultSet := make([]map[string]interface{}, 0)
		colValues := make([]interface{}, len(columns))
		colPointers := make([]interface{}, len(columns))

		// 创建指针切片以接收数据
		for i := range colValues {
			colPointers[i] = &colValues[i]
		}

		// 遍历结果集
		for rows.Next() {
			err = rows.Scan(colPointers...)
			if err != nil {
				return nil, fmt.Errorf("failed to scan row: %v", err)
			}

			// 创建行数据映射
			rowData := make(map[string]interface{})
			for i, colName := range columns {
				val := colPointers[i].(*interface{})
				// 处理特殊类型，如时间和二进制数据
				switch v := (*val).(type) {
				case []byte:
					// 尝试将[]byte转换为字符串
					rowData[colName] = string(v)
				default:
					rowData[colName] = *val
				}
			}

			resultSet = append(resultSet, rowData)
		}

		// 检查遍历过程中是否有错误
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("error during row iteration: %v", err)
		}

		// 将结果转换为JSON
		resultJSON, err := json.MarshalIndent(resultSet, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal result to JSON: %v", err)
		}

		return mcp.NewToolResultText(string(resultJSON)), nil
	} else {
		// 执行非查询语句（如INSERT, UPDATE, DELETE等）
		result, err := db.ExecContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("non-query execution failed: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()
		lastInsertID, _ := result.LastInsertId()

		response := fmt.Sprintf("Query executed successfully. Rows affected: %d", rowsAffected)
		if lastInsertID > 0 {
			response += fmt.Sprintf(", Last insert ID: %d", lastInsertID)
		}

		return mcp.NewToolResultText(response), nil
	}
}
