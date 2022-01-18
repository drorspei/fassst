package fs

import (
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MemFS struct {
	History  []string
	Contents map[string][]byte
	mutex    *sync.Mutex
}

func (fs MemFS) ReadDir(path string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	var dirs []DirEntry
	var files []FileEntry
	for _, key := range fs.History {
		if !strings.HasPrefix(key, path) {
			continue
		}
		if c := fs.Contents[key]; c == nil {
			dirs = append(dirs, NewSimpleEntry(key, true))
		} else {
			files = append(files, NewSimpleEntry(key, false))
		}
	}
	return dirs, files, nil, nil
}

func (fs MemFS) ReadFile(path string) ([]byte, error) {
	fs.mutex.Lock()
	defer func() { fs.mutex.Unlock() }()

	if bs, ok := fs.Contents[path]; !ok {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	} else {
		if bs == nil {
			return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EISDIR}
		}
		return bs, nil
	}
}

func (fs *MemFS) WriteFile(path string, content []byte, modTime time.Time) error {
	fs.mutex.Lock()
	defer func() { fs.mutex.Unlock() }()
	fs.History = append(fs.History, path)
	fs.Contents[path] = content
	return nil
}

func (fs *MemFS) Mkdir(path string) error {
	fs.mutex.Lock()
	defer func() { fs.mutex.Unlock() }()
	if bs, ok := fs.Contents[path]; ok && bs != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}
	fs.History = append(fs.History, path)
	fs.Contents[path] = nil
	return nil
}
