package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func EmbedQuery(query string) []float32 {

	// 使用 map 构建请求参数
	requestBody := map[string]string{
		"model":           "BAAI/bge-m3",
		"input":           query,
		"encoding_format": "float",
	}

	// 将 map 转换为 JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	payload := bytes.NewBuffer(jsonData)

	req, _ := http.NewRequest("POST", os.Getenv("SILICONFLOW_URL"), payload)

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", os.Getenv("SILICONFLOW_TOKEN")))
	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		var response map[string]interface{}
		if err := json.Unmarshal(body, &response); err != nil {
			log.Fatalf("Error parsing JSON response: %v", err)
		}
		log.Fatalf("Request failed with status code: %d", res.StatusCode)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	// 解析 JSON 响应
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		log.Fatalf("Error parsing JSON response: %v", err)
	}

	// 提取嵌入向量
	data, ok := response["data"].([]interface{})
	if !ok {
		log.Fatal("Invalid response format: data field not found or not an array")
	}

	if len(data) == 0 {
		log.Fatal("Empty data array in response")
	}

	firstItem, ok := data[0].(map[string]interface{})
	if !ok {
		log.Fatal("Invalid response format: first data item is not an object")
	}

	embeddingData, ok := firstItem["embedding"].([]interface{})
	if !ok {
		log.Fatal("Invalid response format: embedding field not found or not an array")
	}

	// 将 interface{} 类型的切片转换为 float32 类型的切片
	embeddings := make([]float32, len(embeddingData))
	for i, v := range embeddingData {
		if f, ok := v.(float64); ok {
			embeddings[i] = float32(f)
		} else {
			log.Fatal("Invalid embedding value type: expected float64")
		}
	}
	return embeddings
}
