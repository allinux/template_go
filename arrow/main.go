package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func create_client(profileName string) *s3.Client {
	log.Println("load config")
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(profileName))

	if err != nil {
		log.Fatal(err)
	}
	log.Println("create s3 client")
	return s3.NewFromConfig(cfg)
}

func UploadS3FileFromBuffer(objectKey string, bucket string, s3Client *s3.Client, data []byte) error {
	uploader := manager.NewUploader(s3Client)
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &objectKey,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}
	return nil
}

func DownloadS3FileToBuffer(objectKey string, bucket string, s3Client *s3.Client) ([]byte, error) {
	buffer := manager.NewWriteAtBuffer([]byte{})
	downloader := manager.NewDownloader(s3Client)
	numBytes, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectKey,
	})
	if err != nil {
		return nil, err
	}

	if numBytes < 1 {
		return nil, errors.New("zero bytes written to memory")
	}

	return buffer.Bytes(), nil
}

func main() {
    s3client := create_client("")
	contents, _ := DownloadS3File("sample.parquet", "yhjung-data", s3client)
	rf := bytes.NewReader(contents)

    rdr, _ := file.NewParquetReader(rf)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	arrowRdr, _ := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{Parallel: true}, mem)
	tbl, _ := arrowRdr.ReadTable(context.Background())
	defer tbl.Release()

    for i, f := range tbl.Schema().Fields() {
		fmt.Println(i, f.Name, f.Type)
    }
}
