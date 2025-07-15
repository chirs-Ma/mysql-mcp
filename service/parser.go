package service

import (
	"context"
	"os"

	"github.com/cloudwego/eino-ext/components/document/parser/html"
	"github.com/cloudwego/eino-ext/components/document/parser/pdf"
	"github.com/cloudwego/eino/components/document/parser"
	"github.com/cloudwego/eino/schema"
)

func Parser(ctx context.Context, filePath string) ([]*schema.Document, error) {
	textParser := parser.TextParser{}

	htmlParser, err := html.NewParser(ctx, &html.Config{})
	if err != nil {
		Logger.Errorw("HTML解析器初始化失败", "error", err)
		return nil, err
	}

	pdfParser, err := pdf.NewPDFParser(ctx, &pdf.Config{})
	if err != nil {
		Logger.Errorw("PDF解析器初始化失败", "error", err)
		return nil, err
	}

	// 创建扩展解析器
	extParser, err := parser.NewExtParser(ctx, &parser.ExtParserConfig{
		// 注册特定扩展名的解析器
		Parsers: map[string]parser.Parser{
			".html": htmlParser,
			".pdf":  pdfParser,
		},
		// 设置默认解析器，用于处理未知格式
		FallbackParser: textParser,
	})
	if err != nil {
		Logger.Errorw("扩展解析器初始化失败", "error", err)
		return nil, err
	}

	// 使用解析器
	file, err := os.Open(filePath)
	if err != nil {
		Logger.Errorw("文件打开失败", "filePath", filePath, "error", err)
		return nil, err
	}
	defer file.Close()

	docs, err := extParser.Parse(ctx, file,
		// 必须提供 URI ExtParser 选择正确的解析器进行解析
		parser.WithURI(filePath),
		parser.WithExtraMeta(map[string]any{
			"source": filePath,
		}),
	)
	if err != nil {
		Logger.Errorw("文档解析失败", "filePath", filePath, "error", err)
		return nil, err
	}

	return docs, nil
}
