package main

import (
	"context"
	"database/sql"
	"fmt"
	"mcp-mysql/service"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 全局数据库连接变量
var db *sql.DB
var cli *milvusclient.Client
var logger *zap.SugaredLogger

// 初始化日志
func initLogger() {
	// 创建日志目录
	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(fmt.Sprintf("无法创建日志目录: %v", err))
	}
	// 创建自定义的编码器配置
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// 创建配置
	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderConfig,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
	}

	// 构建日志
	zapLogger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("无法初始化日志: %v", err))
	}

	// 使用SugaredLogger，它提供了类似于fmt.Printf的API
	logger = zapLogger.Sugar()
}

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
	service.InitMilvusConfig(os.Getenv("MILVUS_COLLECTION"))
	return nil
}

func initVectorDB(ctx context.Context, cli *milvusclient.Client) error {
	hasCollection, err := service.CheckCollection(ctx, cli)
	if err != nil {
		logger.Fatalf("CheckCollection failed: %v", err)
	}
	if !hasCollection {
		err = service.CreateCollection(ctx, cli, service.Config.CollectionName)
		if err != nil {
			logger.Fatalf("CreateCollection failed: %v", err)
		}

		// 创建带缓冲的通道
		schemaChan := make(chan string, 10)

		// 启动一个协程获取所有表结构
		go func() {
			service.GetAllTableSchema(ctx, db, schemaChan)
		}()

		// 创建工作池处理表结构
		var wg sync.WaitGroup
		const maxWorkers = 5

		// 信号量控制并发数
		semaphore := make(chan struct{}, maxWorkers)

		// 处理表结构
		for schema := range schemaChan {
			if schema == "" {
				continue
			}

			// 获取信号量
			semaphore <- struct{}{}

			wg.Add(1)
			go func(s string) {
				defer wg.Done()
				defer func() { <-semaphore }() // 释放信号量

				vectors, err := service.EmbedQuery(s)
				if err != nil {
					logger.Errorw("向量嵌入失败", "error", err)
					return
				}

				err = service.SaveToVDB(ctx, cli, []string{s}, [][]float32{vectors})
				if err != nil {
					logger.Errorw("保存向量失败", "error", err)
				}
			}(schema)
		}

		// 等待所有工作完成
		wg.Wait()
		logger.Info("所有表结构向量化处理完成")
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
	// 初始化日志
	initLogger()
	service.Logger = logger
	defer logger.Sync() // 确保缓冲的日志被写入

	// 在 main 函数中
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 添加信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("接收到终止信号，准备关闭...")
		cancel()
	}()

	// 加载.env文件
	err := godotenv.Load(filepath.Join(filepath.Dir(os.Args[0]), ".env"))
	if err != nil {
		logger.Fatalf("警告: 无法加载.env文件: %v", err)
	}

	// 初始化数据库连接
	dsn := buildDSNFromEnv()
	logger.Info("正在连接MySQL数据库...")
	if err = initDB(dsn); err != nil {
		logger.Fatalf("数据库初始化失败: %v", err)
	}
	logger.Info("成功连接到MySQL数据库")
	defer db.Close()

	// 初始化Milvus连接
	if err = initMilvus(); err != nil {
		logger.Fatalf("Milvus初始化失败: %v", err)
	}
	defer cli.Close(context.Background())

	initVectorDB(ctx, cli)

	// Create a new MCP server
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
	logger.Info("启动MCP服务器...")
	if err := server.ServeStdio(s); err != nil {
		logger.Errorf("服务器错误: %v", err)
	}
}

func executeSql(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := request.Params.Arguments["query"].(string)
	logger.Infof("执行查询: %s", query)
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
	logger.Infof("执行查询: %s", query)
	if query == "" {
		return nil, fmt.Errorf("query is empty")
	}
	vectors, err := service.EmbedQuery(query)
	if err != nil {
		return nil, err
	}

	res, err := service.SimilaritySearch(ctx, cli, vectors)
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(res), nil
}
