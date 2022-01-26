package fs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

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

func NewS3FS(url string, ctx context.Context) (S3FS, error) {
	bucket := strings.SplitN(url[len("s3://"):], "/", 2)[0]

	var res S3FS
	res.ctx = ctx
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(res.ctx)
	if err != nil {
		return res, fmt.Errorf("load default config: %w", err)
	}

	// For AWS logs use this:
	// cfg.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	loc, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
	if err != nil {
		return res, fmt.Errorf("get bucket location: %w", err)
	}

	cfg2, err := config.LoadDefaultConfig(res.ctx, config.WithRegion(string(loc.LocationConstraint)))
	if err != nil {
		return res, fmt.Errorf("load default config 2: %w", err)
	}
	res.client = s3.NewFromConfig(cfg2)

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
		files = append(files, NewSimpleEntryTimeSize(bucket+"/"+*object.Key, false, object.Size, *object.LastModified))
		// log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
	for _, object := range output.CommonPrefixes {
		dirs = append(dirs, NewSimpleEntryTimeSize(MakeSureHasSuffix(bucket+"/"+*object.Prefix, "/"), true, 0, time.Time{}))
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
	parts := strings.SplitN(key, "/", 2)
	bucket := parts[0]
	prefix := parts[1]

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

func (fs S3FS) ReadFile(path string) (io.ReadCloser, error) {
	parts := strings.SplitN(path, "/", 2)

	output, err := fs.client.GetObject(fs.ctx, &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get object: %w", err)
	}
	// buf := new(bytes.Buffer)
	// if _, err := buf.ReadFrom(output.Body); err != nil {
	// 	return nil, fmt.Errorf("s3 read object: %w", err)
	// }
	return output.Body, nil
}

func (fs S3FS) WriteFile(path string, content io.Reader, modTime time.Time) (int, error) {
	return 0, fmt.Errorf("s3 write not implemented")
}

func (fs S3FS) Mkdir(path string) error {
	return fmt.Errorf("s3 mkdir not implemented")
}
