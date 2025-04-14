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

// 全局变量
var (
	db     *sql.DB
	cli    *milvusclient.Client
	logger *zap.SugaredLogger
)

// AppConfig 应用配置结构体
type AppConfig struct {
	DB struct {
		User     string
		Password string
		Host     string
		Port     string
		Name     string
		Params   string
	}
	Milvus struct {
		Host       string
		Port       string
		Collection string
	}
	SiliconFlow struct {
		Token string
		URL   string
	}
}

// Config 全局配置实例
var Config AppConfig

// 初始化日志
func initLogger() error {
	// 创建日志目录
	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("无法创建日志目录: %v", err)
	}

	// 创建自定义的编码器配置
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// 创建标准输出和文件输出
	stdoutCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		zap.InfoLevel,
	)

	// 创建日志文件
	logFile := filepath.Join(logDir, "app.log")
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("无法创建日志文件: %v", err)
	}

	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(file),
		zap.InfoLevel,
	)

	// 组合多个输出
	core := zapcore.NewTee(stdoutCore, fileCore)
	zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))

	// 使用SugaredLogger，它提供了类似于fmt.Printf的API
	logger = zapLogger.Sugar()
	return nil
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

func initMilvus(ctx context.Context) error {
	milvusAddress := Config.Milvus.Host + ":" + Config.Milvus.Port
	var err error
	// 设置连接超时上下文
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cli, err = milvusclient.New(connCtx, &milvusclient.ClientConfig{
		Address: milvusAddress,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to Milvus: %v", err)
	}

	service.InitMilvusConfig(Config.Milvus.Collection)
	return nil
}

func initVectorDB(ctx context.Context, cli *milvusclient.Client) error {
	hasCollection, err := service.CheckCollection(ctx, cli)
	if err != nil {
		return fmt.Errorf("CheckCollection failed: %v", err)
	}

	if !hasCollection {
		err = service.CreateCollection(ctx, cli, service.Config.CollectionName)
		if err != nil {
			return fmt.Errorf("CreateCollection failed: %v", err)
		}

		// 创建带缓冲的通道
		schemaChan := make(chan string, 10)

		// 创建子上下文用于控制goroutine生命周期
		workCtx, workCancel := context.WithCancel(ctx)
		defer workCancel() // 确保函数退出时取消所有子goroutine

		// 启动一个协程获取所有表结构
		go func() {
			service.GetAllTableSchema(workCtx, db, schemaChan)
		}()

		// 创建工作池处理表结构
		var wg sync.WaitGroup
		const maxWorkers = 5

		// 信号量控制并发数
		semaphore := make(chan struct{}, maxWorkers)

		// 处理表结构
		for schema := range schemaChan {
			select {
			case <-ctx.Done():
				logger.Info("上下文取消，停止处理表结构")
				return ctx.Err()
			default:
				if schema == "" {
					continue
				}

				// 获取信号量
				semaphore <- struct{}{}

				wg.Add(1)
				go func(s string) {
					defer wg.Done()
					defer func() { <-semaphore }() // 释放信号量

					// 检查上下文是否已取消
					select {
					case <-workCtx.Done():
						return
					default:
						// 继续处理
					}

					vectors, err := service.EmbedQuery(s)
					if err != nil {
						logger.Errorw("向量嵌入失败", "error", err)
						return
					}

					err = service.SaveToVDB(workCtx, cli, []string{s}, [][]float32{vectors})
					if err != nil {
						logger.Errorw("保存向量失败", "error", err)
					}
				}(schema)
			}
		}

		// 等待所有工作完成
		wg.Wait()
		logger.Info("所有表结构向量化处理完成")
	}

	return nil

	return nil
}

// 从配置加载环境变量
func loadConfig() error {
	// 加载数据库配置
	Config.DB.User = os.Getenv("DB_USER")
	Config.DB.Password = os.Getenv("DB_PASSWORD")
	Config.DB.Host = os.Getenv("DB_HOST")
	Config.DB.Port = os.Getenv("DB_PORT")
	Config.DB.Name = os.Getenv("DB_NAME")
	Config.DB.Params = os.Getenv("DB_PARAMS")

	// 加载Milvus配置
	Config.Milvus.Host = os.Getenv("MILVUS_HOST")
	Config.Milvus.Port = os.Getenv("MILVUS_PORT")
	Config.Milvus.Collection = os.Getenv("MILVUS_COLLECTION")

	// 加载SiliconFlow配置
	Config.SiliconFlow.Token = os.Getenv("SILICONFLOW_TOKEN")
	Config.SiliconFlow.URL = os.Getenv("SILICONFLOW_URL")

	// 验证必要的配置
	if Config.DB.User == "" || Config.DB.Host == "" || Config.DB.Name == "" {
		return fmt.Errorf("数据库配置不完整")
	}
	if Config.Milvus.Host == "" || Config.Milvus.Collection == "" {
		return fmt.Errorf("Milvus配置不完整")
	}

	return nil
}

// 从配置构建DSN字符串
func buildDSNFromConfig() string {
	// 构建DSN字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		Config.DB.User,
		Config.DB.Password,
		Config.DB.Host,
		Config.DB.Port,
		Config.DB.Name)

	if Config.DB.Params != "" {
		dsn += "?" + Config.DB.Params
	}

	return dsn
}

func main() {
	// 创建根上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 添加信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("接收到终止信号，准备关闭...")
		cancel()
	}()

	// 初始化日志
	if err := initLogger(); err != nil {
		fmt.Printf("日志初始化失败: %v\n", err)
		os.Exit(1)
	}
	service.Logger = logger
	defer logger.Sync() // 确保缓冲的日志被写入

	// 加载.env文件
	envPath := filepath.Join(filepath.Dir(os.Args[0]), ".env")
	err := godotenv.Load(envPath)
	if err != nil {
		logger.Warnf("无法加载.env文件(%s): %v，尝试使用环境变量", envPath, err)
	}

	// 加载配置
	if err := loadConfig(); err != nil {
		logger.Fatalf("配置加载失败: %v", err)
	}

	// 初始化数据库连接
	dsn := buildDSNFromConfig()
	logger.Info("正在连接MySQL数据库...")
	if err = initDB(dsn); err != nil {
		logger.Fatalf("数据库初始化失败: %v", err)
	}
	logger.Info("成功连接到MySQL数据库")
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	// 初始化Milvus连接
	if err = initMilvus(ctx); err != nil {
		logger.Fatalf("Milvus初始化失败: %v", err)
	}
	defer func() {
		if cli != nil {
			cli.Close(context.Background())
		}
	}()

	// 初始化向量数据库
	if err := initVectorDB(ctx, cli); err != nil {
		logger.Fatalf("向量数据库初始化失败: %v", err)
	}

	// Create a new MCP server
	s := server.NewMCPServer(
		"mcp-mysql",
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

	// 创建带超时的上下文
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := service.Execute(queryCtx, db, query)
	if err != nil {
		logger.Errorw("SQL执行失败", "query", query, "error", err)
		return nil, err
	}

	return mcp.NewToolResultText(res), nil
}

func getCanUseTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := request.Params.Arguments["query"].(string)
	logger.Infof("执行相似度查询: %s", query)
	if query == "" {
		return nil, fmt.Errorf("query is empty")
	}

	// 创建带超时的上下文
	searchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	vectors, err := service.EmbedQuery(query)
	if err != nil {
		logger.Errorw("向量嵌入失败", "query", query, "error", err)
		return nil, fmt.Errorf("向量嵌入失败: %w", err)
	}

	res, err := service.SimilaritySearch(searchCtx, cli, vectors)
	if err != nil {
		logger.Errorw("相似度搜索失败", "query", query, "error", err)
		return nil, fmt.Errorf("相似度搜索失败: %w", err)
	}

	return mcp.NewToolResultText(res), nil
}
