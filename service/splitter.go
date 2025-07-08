package service

import (
	"context"
	"strings"

	"github.com/cloudwego/eino-ext/components/document/transformer/splitter/recursive"
	"github.com/cloudwego/eino/schema"
)

func Splitter(ctx context.Context, docs []*schema.Document) ([]*schema.Document, error) {
	// 初始化分割器
	splitter, err := recursive.NewSplitter(ctx, &recursive.Config{
		ChunkSize:   10,                            // 必需：目标片段大小
		OverlapSize: 20,                            // 可选：片段重叠大小
		Separators:  []string{"\n", ".", "?", "!"}, // 可选：分隔符列表
		LenFunc: func(s string) int {
			// eg: 使用 unicode 字符数而不是字节数，并去除空格
			cleanedStr := strings.ReplaceAll(s, " ", "")
			return len([]rune(cleanedStr))
		}, // 可选：自定义长度计算函数
		KeepType: recursive.KeepTypeNone, // 可选：分隔符保留策略
	})
	if err != nil {
		panic(err)
	}
	// 执行分割
	results, err := splitter.Transform(ctx, docs)
	if err != nil {
		panic(err)
	}

	// 处理分割结果
	for i, doc := range results {
		println("片段", i+1, ":", doc.Content)
	}
	return results, nil
}
