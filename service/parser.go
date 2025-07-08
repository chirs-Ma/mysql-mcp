package service

import (
	"context"
	"os"

	"github.com/cloudwego/eino-ext/components/document/parser/html"
	"github.com/cloudwego/eino-ext/components/document/parser/pdf"
	"github.com/cloudwego/eino/components/document/parser"
	"github.com/cloudwego/eino/schema"
	"github.com/cloudwego/hertz/cmd/hz/util/logs"
)

func Parser(ctx context.Context, filePath string) ([]*schema.Document, error) {
	textParser := parser.TextParser{}

	htmlParser, _ := html.NewParser(ctx, &html.Config{})

	pdfParser, _ := pdf.NewPDFParser(ctx, &pdf.Config{})

	// 创建扩展解析器
	extParser, _ := parser.NewExtParser(ctx, &parser.ExtParserConfig{
		// 注册特定扩展名的解析器
		Parsers: map[string]parser.Parser{
			".html": htmlParser,
			".pdf":  pdfParser,
		},
		// 设置默认解析器，用于处理未知格式
		FallbackParser: textParser,
	})

	// 使用解析器
	file, _ := os.Open(filePath)

	docs, _ := extParser.Parse(ctx, file,
		// 必须提供 URI ExtParser 选择正确的解析器进行解析
		parser.WithURI(filePath),
		parser.WithExtraMeta(map[string]any{
			"source": "local",
		}),
	)

	for idx, doc := range docs {
		logs.Infof("doc_%v content: %v", idx, doc.Content)
	}
	return docs, nil
}
