package main

import (
	"context"
	"fmt"
	"strconv"

	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const page = 1000

var parallel = 50

type Unit = struct{}

var U = Unit{}

type FileEntry interface {
	IsDir() bool
	Name() string
}

type FileSystem interface {
	ReadDir(string, string) ([]FileEntry, error)
	Stat(string) (FileEntry, error)
}

// type LocalFS struct{}

// func (fs LocalFS) ReadDir(key string) ([]FileEntry, error) {
// 	dirents, err := os.ReadDir(key)
// 	if err != nil {
// 		return nil, fmt.Errorf("read dir: %w", err)
// 	}
// 	var results []FileEntry
// 	for _, de := range dirents {
// 		results = append(results, de)
// 	}
// 	return results, nil
// }
// func (fs LocalFS) Stat(key string) (FileEntry, error) {
// 	info, err := os.Stat(key)
// 	if err != nil {
// 		return nil, fmt.Errorf("stat: %w", err)
// 	}
// 	return info, nil
// }

type S3FileEntry struct {
	Path string
}

func (f S3FileEntry) Name() string {
	return f.Path
}
func (f S3FileEntry) IsDir() bool {
	return strings.HasSuffix(f.Path, "/")
}

type S3FS struct {
	client *s3.Client
	ctx    context.Context
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

func (fs S3FS) ReadDir(key, startAfter string) ([]FileEntry, error) {
	// Get the first page of results for ListObjectsV2 for a bucket
	parts := strings.Split(key, "/")
	prefix := strings.Join(parts[1:], "/")

	output, err := fs.client.ListObjectsV2(fs.ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(parts[0]),
		Prefix:     aws.String(prefix),
		Delimiter:  aws.String("/"),
		StartAfter: aws.String(startAfter),
	})
	if err != nil {
		// log.Fatal(err)
		return nil, fmt.Errorf("client list objects: %w", err)
	}

	var results []FileEntry

	// log.Println("first page results:")
	for _, object := range output.Contents {
		results = append(results, S3FileEntry{(*object.Key)[len(prefix):]})
		// log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
	for _, object := range output.CommonPrefixes {
		results = append(results, S3FileEntry{(*object.Prefix)[len(prefix):]})
		// log.Printf("subdir=%s", aws.ToString(object.Prefix))
	}

	return results, nil
}

func (fs S3FS) Stat(key string) (FileEntry, error) {
	return S3FileEntry{key}, nil
}

func main() {
	if len(os.Args) > 2 {
		var err error
		if parallel, err = strconv.Atoi(os.Args[2]); err != nil {
			panic(err)
		}
	}
	// var fs LocalFS
	fs, err := NewS3FS(context.Background())
	if err != nil {
		panic(err)
	}

	t0 := time.Now()
	res, err := run(fs, os.Args[1], parallel, page)
	t1 := time.Now()
	if err != nil {
		panic(err)
	}
	os.WriteFile("toc2.txt", []byte(strings.Join(res, "\n")), 0644)
	t2 := time.Now()
	fmt.Println("Done :)")
	fmt.Println("run", t1.Sub(t0))
	fmt.Println("save", t2.Sub(t1))
}

func run(fs FileSystem, startPath string, routines, pageSize int) ([]string, error) {
	drainChan := make(chan string, routines*pageSize)
	nameChan := make(chan string, routines)
	runChan := make(chan Unit, routines)
	errChan := make(chan error)

	nameMap := make(map[string]Unit, routines)
	var results []string

	uid := uuid.New().String()
	nameChan <- uid
	go foo(fs, startPath, "", uid, runChan, nameChan, drainChan, errChan)

	go func() {
		for {
			select {
			case v := <-drainChan:
				results = append(results, v)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	var stop bool
	for !stop {
		select {
		case err := <-errChan:
			return nil, err
		case name := <-nameChan:
			if _, ok := nameMap[name]; ok {
				delete(nameMap, name)
			} else {
				nameMap[name] = U
			}
			if len(nameMap) == 0 {
				stop = true
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for len(drainChan) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	return results, nil
}

func foo(fs FileSystem, url, startAfter, name string, runChan chan Unit, nameChan, drainChan chan string, errChan chan error) {
	runChan <- U
	defer func() { <-runChan }()

	ds, files, last, hasMore, err := Query(fs, url, startAfter)
	if err != nil {
		errChan <- err
		return
	}
	if hasMore {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(fs, url, last, uid, runChan, nameChan, drainChan, errChan)
	}
	for _, d := range ds {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(fs, d, "", uid, runChan, nameChan, drainChan, errChan)
	}
	for _, f := range files {
		drainChan <- f
	}

	nameChan <- name
}

func Query(fs FileSystem, key, startAfter string) ([]string, []string, string, bool, error) {
	// fmt.Println(key)
	info, err := fs.Stat(key)
	var cont bool
	var startName string
	if err != nil || info == nil || !info.IsDir() {
		key, startName = path.Split(key)
		cont = true
	} else if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	dirents, err := fs.ReadDir(key, startAfter)
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("read dir '%s': %w", key, err)
	}
	var files []string
	var dirs []string
	count := 0
	var last string
	for _, d := range dirents {
		if cont && startName >= d.Name() {
			continue
		}
		p := fmt.Sprintf("%s%s", key, d.Name())
		if d.IsDir() {
			if !strings.HasSuffix(p, "/") {
				p += "/"
			}
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}
		count++
		if count >= page {
			last = p
			break
		}
	}
	return dirs, files, last, count >= page, nil
}

