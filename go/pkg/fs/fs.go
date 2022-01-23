package fs

import (
	"context"
	"fassst/pkg/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Version = "v0.5.0"

func MakeSureHasSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s
	}
	return s + suffix
}

type FileEntry interface {
	Name() string
	Size() int64
	ModTime() time.Time
	IsDir() bool
	Mode() os.FileMode
	Sys() interface{}
}

type DirEntry interface {
	Name() string
}

type Pagination interface{}

type FileSystem interface {
	ReadDir(string, Pagination) ([]DirEntry, []FileEntry, Pagination, error)
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, content []byte, modTime time.Time) error
	Mkdir(path string) error
}

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
		maxcallspersecond, errmaxcallspersecond := strconv.ParseInt(subparts[3], 10, 0)

		if errdepth != nil || errdegree != nil || errpagesize != nil || errmaxcallspersecond != nil {
			return "", nil, fmt.Errorf("mock fs requires depth, degree, page size and max calls per second like mock://5:5:2:1@url")
		}

		return strings.Join(parts[1:], "/"), &MockKTreeFS{
			uint64(depth), uint64(degree), uint64(pagesize),
			uint64(maxcallspersecond), 0,
			[]time.Time{}, &sync.Mutex{},
		}, nil
	}

	if strings.HasPrefix(url, "mem://") {
		return MakeSureHasSuffix(url[len("mem://"):], "/"), &MemFS{
			DirectoryFiles: make(map[string][]SimpleFileEntry),
			Directories:    utils.NewSet("/"),
			Contents:       make(map[string][]byte),
			mutex:          &sync.Mutex{},
		}, nil
	}

	return url, LocalFS{}, nil
}
