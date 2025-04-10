package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"mcp-mysql/service"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// 全局数据库连接变量
var db *sql.DB
var cli *milvusclient.Client

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

func initMilvus() error {
	milvusAddress := os.Getenv("MILVUS_HOST") + ":" + os.Getenv("MILVUS_PORT")
	var err error
	// 设置连接超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err = milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddress,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to Milvus: %v", err)
	}
	return nil
}

func initVectorDB(ctx context.Context, cli *milvusclient.Client) error {
	hasCollection, err := service.CheckCollection(ctx, cli)
	if err != nil {
		log.Fatalf("CheckCollection failed: %v", err)
	}
	if !hasCollection {
		err = service.CreateCollection(ctx, cli, os.Getenv("MILVUS_COLLECTION"))
		if err != nil {
			log.Fatalf("CreateCollection failed: %v", err)
		}
		allTableSchema, err := service.GetAllTableSchema(ctx, db)
		if err != nil {
			log.Fatalf("GetAllTableSchema failed: %v", err.Error())
		}
		for _, schema := range allTableSchema {
			vectors := service.EmbedQuery(schema)
			service.SaveToVDB(ctx, cli, []string{schema}, [][]float32{vectors})
		}
	}

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
	ctx := context.Background()
	// 加载.env文件
	err := godotenv.Load(filepath.Join(filepath.Dir(os.Args[0]), ".env"))
	if err != nil {
		log.Fatalf("警告: 无法加载.env文件: %v\n", err)
	}

	// 初始化数据库连接
	dsn := buildDSNFromEnv()
	log.Println("Connecting to MySQL database...")
	if err = initDB(dsn); err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	log.Println("Successfully connected to MySQL database")
	defer db.Close()

	// 初始化Milvus连接
	if err = initMilvus(); err != nil {
		log.Fatalf("Milvus initialization failed: %v", err)
	}
	defer cli.Close(context.Background())

	initVectorDB(ctx, cli)

	s := server.NewMCPServer(
		"mcp-go",
		"1.0.0",
	)
	// Add tool
	getCanUseTabletool := mcp.NewTool("get_can_use_table",
		mcp.WithDescription("Find relevant database tables based on natural language description, used before executing SQL queries"),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("Natural language query description"),
		),
	)

	executeSqltool := mcp.NewTool("execute_sql",
		mcp.WithDescription("Execute SQL query statements on MySQL database and return the results"),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute"),
		),
	)

	// Add tool handler
	s.AddTool(getCanUseTabletool, getCanUseTable)
	s.AddTool(executeSqltool, executeSql)

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

	res, err := service.Execute(ctx, db, query)
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(res), nil

}
func getCanUseTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := request.Params.Arguments["query"].(string)
	fmt.Printf("Executing query: %s\n", query)
	if query == "" {
		return nil, fmt.Errorf("query is empty")
	}
	vectors := service.EmbedQuery(query)

	res, err := service.SimilaritySearch(ctx, cli, vectors)
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(res), nil
}
