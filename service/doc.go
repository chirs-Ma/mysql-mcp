package service

import (
	"context"
	"path/filepath"

	file "github.com/cloudwego/eino-ext/components/document/loader/file"
	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/schema"
)

func LoadDoc(ctx context.Context, path string) ([]*schema.Document, error) {
	// 获取文件后缀
	ext := filepath.Ext(path)
	if ext == "pdf" || ext == "html" {
		docs, err := Parser(ctx, path)
		if err != nil {
			return nil, err
		}
		return docs, nil
	}

	loader, err := file.NewFileLoader(ctx, &file.FileLoaderConfig{
		UseNameAsID: true,
	})
	if err != nil {
		Logger.Errorw("文件加载器初始化失败", "error", err)
		return nil, err
	}

	// 加载文档
	docs, err := loader.Load(ctx, document.Source{
		URI: path,
	})
	if err != nil {
		Logger.Errorw("文档加载失败", "path", path, "error", err)
		return nil, err
	}

	splitterDocs, err := Splitter(ctx, docs)
	if err != nil {
		Logger.Errorw("文档分割失败", "error", err)
		return nil, err
	}
	return splitterDocs, nil
}
