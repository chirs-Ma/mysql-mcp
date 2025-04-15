package service

import (
	"context"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"go.uber.org/zap"
)

const (
	dim         = 1024
	searchLimit = 3 // 搜索结果限制数量
)

// 全局日志变量，由 main 包初始化
var Logger *zap.SugaredLogger

func CreateCollection(ctx context.Context, cli *milvusclient.Client, collectionName string) error {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("my_id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("vector").WithDim(dim).WithDataType(entity.FieldTypeFloatVector)).
		WithField(entity.NewField().WithName("schema").WithDataType(entity.FieldTypeVarChar).WithMaxLength(10240))

	err := cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	if err != nil {
		Logger.Errorw("创建集合失败", "error", err, "collection", collectionName)
		return err
	}
	index := index.NewAutoIndex(entity.COSINE)
	indexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "vector", index))
	if err != nil {
		Logger.Errorw("创建索引失败", "error", err, "collection", collectionName)
		return err
	}

	err = indexTask.Await(ctx)
	if err != nil {
		Logger.Errorw("等待索引创建完成失败", "error", err, "collection", collectionName)
		return err
	}
	loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		Logger.Errorw("加载集合失败", "error", err, "collection", collectionName)
		return err
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		Logger.Errorw("等待集合加载完成失败", "error", err, "collection", collectionName)
		return err
	}

	Logger.Info("集合创建成功", "collection", collectionName)
	return nil
}

// MilvusConfig 存储 Milvus 相关配置
type MilvusConfig struct {
	CollectionName string
	// 可以添加其他配置项，如维度、搜索限制等
	Dimension   int
	SearchLimit int
}

// 全局配置变量
var Config MilvusConfig

// 初始化配置
func InitMilvusConfig(collectionName string) {
	Config = MilvusConfig{
		CollectionName: collectionName,
		Dimension:      dim,
		SearchLimit:    3,
	}
}

// CheckCollection 检查集合是否存在
func CheckCollection(ctx context.Context, cli *milvusclient.Client) (has bool, err error) {
	// 使用配置中的集合名称
	has, err = cli.HasCollection(ctx, milvusclient.NewHasCollectionOption(Config.CollectionName))
	if err != nil {
		Logger.Errorw("检查集合是否存在失败", "error", err, "collection", Config.CollectionName)
		return false, err
	}
	return has, err
}

// SaveToVDB 保存数据到向量数据库
func SaveToVDB(ctx context.Context, cli *milvusclient.Client, schemas []string, vector [][]float32) (err error) {
	resp, err := cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(Config.CollectionName).
		WithVarcharColumn("schema", schemas).
		WithFloatVectorColumn("vector", dim, vector),
	)
	if err != nil {
		Logger.Errorw("插入数据失败", "error", err)
		return
	}
	Logger.Infow("数据插入成功", "insertCount", resp.InsertCount, "idsLen", resp.IDs.Len())

	return nil
}

// SimilaritySearch 执行相似度搜索
func SimilaritySearch(ctx context.Context, cli *milvusclient.Client, queryVector []float32) (string, error) {
	stats, err := cli.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(Config.CollectionName))
	if err != nil {
		Logger.Errorw("获取集合统计信息失败", "error", err)
		return "", err
	}
	if stats["row_count"] == "0" {
		loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(Config.CollectionName))
		if err != nil {
			Logger.Errorw("加载集合失败", "error", err)
			return "", err
		}

		// sync wait collection to be loaded
		err = loadTask.Await(ctx)
		if err != nil {
			Logger.Errorw("等待集合加载完成失败", "error", err)
			return "", err
		}
	}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		Config.CollectionName,
		Config.SearchLimit,
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithOutputFields("schema"))
	if err != nil {
		Logger.Errorw("执行相似度搜索失败", "error", err)
		return "", err
	}

	res := ""
	for _, resultSet := range resultSets {
		Logger.Debugw("搜索结果集", "idsLen", resultSet.IDs.Len(), "scores", resultSet.Scores)
		for _, result := range resultSet.Fields {
			fileData := result.FieldData().GetScalars().GetStringData().GetData()
			for _, v := range fileData {
				res += v
			}
		}
	}

	return res, nil
}
