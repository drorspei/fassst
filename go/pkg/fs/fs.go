package fs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func MakeSureHasSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s
	}
	return s + suffix
}

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
			uint64(depth), uint64(degree), uint64(pagesize), uint64(maxcallspersecond), 0,
			&global_recent_mock_calls, &global_recent_mock_calls_mutex,
		}, nil
	}

	return url, LocalFS{}, nil
}
