package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// EmbeddingRequest 表示嵌入请求的结构
type EmbeddingRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	EncodingFormat string `json:"encoding_format"`
}

// EmbeddingResponse 表示嵌入响应的结构
type EmbeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

// EmbedQuery 将查询文本转换为向量嵌入
func EmbedQuery(query string) ([]float32, error) {
	// 从main包获取配置
	sfURL := os.Getenv("SILICONFLOW_URL")
	sfToken := os.Getenv("SILICONFLOW_TOKEN")

	// 验证配置
	if sfURL == "" || sfToken == "" {
		return nil, fmt.Errorf("SiliconFlow配置不完整")
	}

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 使用结构体构建请求参数
	requestBody := EmbeddingRequest{
		Model:          "BAAI/bge-m3",
		Input:          query,
		EncodingFormat: "float",
	}

	// 将结构体转换为 JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("JSON 序列化失败: %v", err)
	}

	payload := bytes.NewBuffer(jsonData)

	// 创建请求并处理错误
	req, err := http.NewRequestWithContext(ctx, "POST", sfURL, payload)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", sfToken))
	req.Header.Add("Content-Type", "application/json")

	// 使用自定义的 HTTP 客户端，设置超时
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// 发送请求并处理错误
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer res.Body.Close() // 确保响应体被关闭

	// 读取响应体
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查状态码
	if res.StatusCode != http.StatusOK {
		var errorResponse map[string]interface{}
		if err := json.Unmarshal(body, &errorResponse); err != nil {
			return nil, fmt.Errorf("请求失败，状态码: %d", res.StatusCode)
		}
		return nil, fmt.Errorf("请求失败，状态码: %d, 错误: %v", res.StatusCode, errorResponse)
	}

	// 使用结构体解析响应
	var response EmbeddingResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 验证响应数据
	if len(response.Data) == 0 {
		return nil, fmt.Errorf("响应中没有数据")
	}

	// 转换为 float32 数组
	embeddings := make([]float32, len(response.Data[0].Embedding))
	for i, v := range response.Data[0].Embedding {
		embeddings[i] = float32(v)
	}

	return embeddings, nil
}

// UpdateSchema 定时更新数据库表结构
func UpdateSchema(db *sql.DB, cli *milvusclient.Client) {
	// 创建定时器，每隔一段时间执行一次更新
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	var updateMutex sync.Mutex

	// 定时执行
	for range ticker.C {
		// 尝试获取锁，如果已经在执行则跳过本次更新
		if !updateMutex.TryLock() {
			Logger.Warn("上一次更新任务仍在进行中，跳过本次更新")
			continue
		}
		defer updateMutex.Unlock()
		tableCh := make(chan map[string]string, 10)
		GetAllTableSchema(context.Background(), db, tableCh)

		for tableMap := range tableCh {
			for tableName, schema := range tableMap {
				notExistTables := CheckRowExist([]string{tableName})
				if len(notExistTables) > 0 {
					// 执行更新操作
					_, err := SaveToSQLite(notExistTables)
					if err != nil {
						Logger.Errorw("数据保存失败", "error", err)
						continue
					}
					vectors, err := EmbedQuery(schema)
					if err != nil {
						Logger.Errorw("向量嵌入失败", "error", err)
						return
					}

					err = SaveToVDB(context.Background(), cli, []string{schema}, [][]float32{vectors})
					if err != nil {
						Logger.Errorw("保存向量失败", "error", err)
					}
				}
			}

		}

	}
}
