package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const page = 1000

var parallel = 30

func MakeSureHasSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s
	}
	return s + suffix
}

type Unit = struct{}

var U = Unit{}

type FileEntry interface {
	Name() string
}

type DirEntry interface {
	Name() string
}

type Pagination interface{}

type FileSystem interface {
	ReadDir(string, Pagination) ([]DirEntry, []FileEntry, Pagination, error)
}

type MockKTreeFS struct {
	Depth                uint64
	Degree               uint64
	PageSize             uint64
	MaxCallsPerSecond    uint64
	RecentMockCalls      *[]time.Time
	RecentMockCallsMutex *sync.Mutex
}

type MockKTreePagination struct {
	Depth uint64
	Start uint64
}

func Min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func (fs MockKTreeFS) AddRecentMockCall(time time.Time) bool {
	fs.RecentMockCallsMutex.Lock()
	defer fs.RecentMockCallsMutex.Unlock()

	i := 0
	for i < len(*fs.RecentMockCalls) && time.Sub((*fs.RecentMockCalls)[i]).Seconds() >= 1 {
		i++
	}

	if i > 0 {
		*fs.RecentMockCalls = (*fs.RecentMockCalls)[i:]
		// global_recent_mock_calls = global_recent_mock_calls[i:]
	}

	if uint64(len(*fs.RecentMockCalls)) < fs.MaxCallsPerSecond {
		// global_recent_mock_calls = append(global_recent_mock_calls, time)
		*fs.RecentMockCalls = append(*fs.RecentMockCalls, time)
		return true
	}

	return false
}

func (fs MockKTreeFS) ReadDir(url string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	if !fs.AddRecentMockCall(time.Now()) {
		return nil, nil, nil, fmt.Errorf("too fast; Error Code: SlowDown")
	}

	var dirs []DirEntry
	var files []FileEntry
	var start uint64 = 0

	for strings.HasPrefix(url, "/") {
		url = url[1:]
	}

	parts := strings.Split(url, "/")
	depth := uint64(len(parts))

	if depth > fs.Depth {
		return nil, nil, nil, fmt.Errorf("url doesn't exist: %s", url)
	}

	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		s, err := strconv.ParseInt(part, 10, 0)
		if err != nil || uint64(s) >= fs.Degree {
			return nil, nil, nil, fmt.Errorf("directory doesn't exist: %s", url)
		}
	}

	if len(parts[len(parts)-1]) > 0 {
		s, err := strconv.ParseInt(parts[len(parts)-1], 10, 0)
		if err != nil || uint64(s) >= fs.Degree {
			return nil, nil, nil, fmt.Errorf("file doesn't exist: %s", url)
		}
		start = uint64(s)
	}

	if pagination != nil {
		p := pagination.(MockKTreePagination)
		depth = p.Depth
		start = p.Start
	}

	if depth > fs.Depth || start >= fs.Degree {
		return nil, nil, nil, fmt.Errorf("pagination doesn't exist: depth=%d start=%d", depth, start)
	}

	base := ""
	if len(parts) > 1 {
		base = strings.Join(parts[:len(parts)-1], "/")
	}

	if depth < fs.Depth {
		for i := start; i < Min(start+fs.PageSize, fs.Degree); i++ {
			dirs = append(dirs, SimpleFileEntry{fmt.Sprintf("%s/%d", base, i)})
		}
	} else {
		for i := start; i < Min(start+fs.PageSize, fs.Degree); i++ {
			files = append(files, SimpleFileEntry{fmt.Sprintf("%s/%d", base, i)})
		}
	}

	if start+fs.PageSize < fs.Degree {
		return dirs, files, MockKTreePagination{depth, start + fs.PageSize}, nil
	}

	return dirs, files, nil, nil
}

type LocalFS struct{}

type SimpleFileEntry struct {
	Path string
}

func (f SimpleFileEntry) Name() string {
	return f.Path
}

func (fs LocalFS) ReadDir(key string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	dirents, err := os.ReadDir(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read dir: %w", err)
	}
	var dirs []DirEntry
	var files []FileEntry
	for _, de := range dirents {
		if de.IsDir() {
			dirs = append(dirs, SimpleFileEntry{MakeSureHasSuffix(key+de.Name(), "/")})
		} else {
			files = append(files, SimpleFileEntry{key + de.Name()})
		}
	}
	return dirs, files, nil, nil
}

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
		var p S3Pagination = pagination.(S3Pagination)

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

var global_recent_mock_calls []time.Time
var global_recent_mock_calls_mutex sync.Mutex

func FileSystemByUrl(url string) (string, FileSystem, error) {
	if strings.HasPrefix(url, "s3://") {
		fs, err := NewS3FS(context.Background())
		return url[len("s3://"):], fs, err
	}

	if strings.HasPrefix(url, "mock://") {
		url = url[len("mock://"):]
		parts := strings.Split(url, "@")
		if len(parts) < 2 {
			return "", nil, fmt.Errorf("mock fs requires depth, degree, page size and max calls per second like mock://5:5:2:1@url")
		}
		subparts := strings.Split(parts[0], ":")
		if len(subparts) != 4 {
			return "", nil, fmt.Errorf("mock fs requires depth, degree, page size and max calls per second like mock://5:5:2:1@url")
		}
		depth, errdepth := strconv.ParseInt(subparts[0], 10, 0)
		degree, errdegree := strconv.ParseInt(subparts[1], 10, 0)
		pagesize, errpagesize := strconv.ParseInt(subparts[2], 10, 0)
		maxcallspersecond, errpagesize := strconv.ParseInt(subparts[3], 10, 0)

		if errdepth != nil || errdegree != nil || errpagesize != nil {
			return "", nil, fmt.Errorf("mock fs requires depth, degree, page size and max calls per second like mock://5:5:2:1@url")
		}

		return strings.Join(parts[1:], "/"), MockKTreeFS{
			uint64(depth), uint64(degree), uint64(pagesize), uint64(maxcallspersecond),
			&global_recent_mock_calls, &global_recent_mock_calls_mutex,
		}, nil
	}

	return url, LocalFS{}, nil
}

func main() {
	if len(os.Args) > 2 {
		var err error
		if parallel, err = strconv.Atoi(os.Args[2]); err != nil {
			panic(err)
		}
	}

	url := MakeSureHasSuffix(os.Args[1], "/")

	// var fs LocalFS
	url, fs, err := FileSystemByUrl(url)
	if err != nil {
		panic(err)
	}

	t0 := time.Now()
	res, err := run(fs, url, parallel, page)
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
	go foo(fs, startPath, uid, nil, runChan, nameChan, drainChan, errChan)

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

func foo(fs FileSystem, url, name string, pagination Pagination, runChan chan Unit, nameChan, drainChan chan string, errChan chan error) {
	runChan <- U
	defer func() { <-runChan }()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		errChan <- err
		return
	}

	if new_pagination != nil {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(fs, url, uid, new_pagination, runChan, nameChan, drainChan, errChan)
	}

	for _, d := range dirs {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(fs, MakeSureHasSuffix(d.Name(), "/"), uid, nil, runChan, nameChan, drainChan, errChan)
	}

	for _, f := range files {
		drainChan <- f.Name()
	}

	nameChan <- name
}

// func Query(fs FileSystem, key, startAfter string) ([]string, []string, string, bool, error) {
// 	// fmt.Println(key)
// 	info, err := fs.Stat(key)
// 	var cont bool
// 	var startName string
// 	if err != nil || info == nil || !info.IsDir() {
// 		key, startName = path.Split(key)
// 		cont = true
// 	} else if !strings.HasSuffix(key, "/") {
// 		key += "/"
// 	}

// 	dirs, files, pagination, err := fs.ReadDir(key)
// 	if err != nil {
// 		return nil, nil, "", false, fmt.Errorf("read dir '%s': %w", key, err)
// 	}
// 	var files []string
// 	var dirs []string
// 	count := 0
// 	var last string
// 	for _, d := range dirents {
// 		if cont && startName >= d.Name() {
// 			continue
// 		}
// 		p := fmt.Sprintf("%s%s", key, d.Name())
// 		if d.IsDir() {
// 			if !strings.HasSuffix(p, "/") {
// 				p += "/"
// 			}
// 			dirs = append(dirs, p)
// 		} else {
// 			files = append(files, p)
// 		}
// 		count++
// 		if count >= page {
// 			last = p
// 			break
// 		}
// 	}
// 	return dirs, files, last, count >= page, nil
// }
