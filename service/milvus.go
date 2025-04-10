package service

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	dim = 1024
)

func CreateCollection(ctx context.Context, cli *milvusclient.Client, collectionName string) error {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("my_id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("vector").WithDim(1024).WithDataType(entity.FieldTypeFloatVector)).
		WithField(entity.NewField().WithName("schema").WithDataType(entity.FieldTypeVarChar).WithMaxLength(10240))

	err := cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	index := index.NewAutoIndex(entity.COSINE)
	indexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "vector", index))
	if err != nil {
		log.Println(err.Error())
		return err
	}

	err = indexTask.Await(ctx)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	fmt.Println("collection created")
	return nil
}

func CheckCollection(ctx context.Context, cli *milvusclient.Client) (has bool, err error) {
	has, err = cli.HasCollection(ctx, milvusclient.NewHasCollectionOption(os.Getenv("MILVUS_COLLECTION")))
	if err != nil {
		log.Println(err.Error())
		return false, err
	}
	return has, err
}

func SaveToVDB(ctx context.Context, cli *milvusclient.Client, schemas []string, vector [][]float32) (err error) {
	resp, err := cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(os.Getenv("MILVUS_COLLECTION")).
		WithVarcharColumn("schema", schemas).
		WithFloatVectorColumn("vector", 1024, vector),
	)
	if err != nil {
		log.Println(err.Error())
		return
	}
	log.Println(resp.InsertCount, resp.IDs.Len())

	return nil
}

func SimilaritySearch(ctx context.Context, cli *milvusclient.Client, queryVector []float32) (string, error) {
	loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(os.Getenv("MILVUS_COLLECTION")))
	if err != nil {
		fmt.Println("load error:" + err.Error())
		return "", err
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		os.Getenv("MILVUS_COLLECTION"), // collectionName
		3,                              // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithOutputFields("schema"))
	if err != nil {
		log.Println("failed to perform basic ANN search collection: ", err.Error())
		return "", err
	}
	res := ""
	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs.Len())
		log.Println("Scores: ", resultSet.Scores)
		for _, result := range resultSet.Fields {
			fileData := result.FieldData().GetScalars().GetStringData().GetData()
			for _, v := range fileData {
				res += v
			}
		}
	}
	return res, nil

}
