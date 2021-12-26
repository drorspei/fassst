package fs

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3FS struct {
	client *s3.Client
	ctx    context.Context
}

type S3Pagination struct {
	Bucket            string
	Prefix            string
	ContinuationToken string
}

func NewS3FS(ctx context.Context) (S3FS, error) {
	var res S3FS
	res.ctx = ctx
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(res.ctx)
	if err != nil {
		return res, fmt.Errorf("load default config: %w", err)
	}

	// Create an Amazon S3 service client
	res.client = s3.NewFromConfig(cfg)
	return res, nil
}

func ReadDirInner(bucket string, prefix string, output *s3.ListObjectsV2Output, err error) ([]DirEntry, []FileEntry, Pagination, error) {
	if err != nil {
		// log.Fatal(err)
		return nil, nil, nil, fmt.Errorf("client list objects: %w", err)
	}

	var dirs []DirEntry
	var files []FileEntry

	// log.Println("first page results:")
	for _, object := range output.Contents {
		files = append(files, SimpleFileEntry{bucket + "/" + *object.Key})
		// log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
	for _, object := range output.CommonPrefixes {
		dirs = append(dirs, SimpleFileEntry{MakeSureHasSuffix(bucket+"/"+*object.Prefix, "/")})
		// log.Printf("subdir=%s", aws.ToString(object.Prefix))
	}

	var pagination Pagination = nil
	if output.NextContinuationToken != nil && *output.NextContinuationToken != "" {
		pagination = S3Pagination{bucket, prefix, *output.NextContinuationToken}
	}

	return dirs, files, pagination, nil
}

// returns dirs, fileentries, pagination, error
func (fs S3FS) ReadDir(key string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	// Get the first page of results for ListObjectsV2 for a bucket
	parts := strings.Split(key, "/")
	bucket := parts[0]
	prefix := strings.Join(parts[1:], "/")

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if pagination != nil {
		p, ok := pagination.(S3Pagination)
		if !ok {
			return nil, nil, pagination, fmt.Errorf("wrong pagination")
		}

		output, err := fs.client.ListObjectsV2(fs.ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: aws.String(p.ContinuationToken),
		})
		return ReadDirInner(bucket, prefix, output, err)
	} else {
		output, err := fs.client.ListObjectsV2(fs.ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(prefix),
			Delimiter: aws.String("/"),
		})
		return ReadDirInner(bucket, prefix, output, err)
	}
}