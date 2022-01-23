package fs

import (
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"fassst/pkg/utils"
)

type MemFS struct {
	DirectoryFiles map[string][]SimpleFileEntry
	Directories    utils.Set
	Contents       map[string][]byte

	mutex *sync.Mutex
}

func (fs MemFS) ReadDir(dirpath string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	var dirs []DirEntry
	var files []FileEntry
	dirpath = MakeSureHasSuffix(dirpath, "/")
	if _, ok := fs.Directories[dirpath]; !ok {
		return nil, nil, nil, &os.PathError{Op: "read", Path: dirpath, Err: syscall.ENOENT}
	}
	if bs, ok := fs.Contents[dirpath]; ok && bs != nil {
		return nil, nil, nil, &os.PathError{Op: "read", Path: dirpath, Err: syscall.ENOTDIR}
	}
	for _, fe := range fs.DirectoryFiles[dirpath] {
		files = append(files, fe)
	}
	for dir := range fs.Directories {
		if strings.HasPrefix(dir, dirpath) && strings.Count(dir, "/") == strings.Count(dirpath, "/")+1 {
			dirs = append(dirs, NewSimpleEntry(dir, true))

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

func (fs *MemFS) WriteFile(filepath string, content []byte, modTime time.Time) error {
	fs.mutex.Lock()
	defer func() { fs.mutex.Unlock() }()
	if !strings.HasPrefix(filepath, "/") {
		filepath = "/" + filepath
	}
	dir := path.Dir(filepath)
	if len(dir) == 0 || dir == "." {
		dir = "/"
	}
	dir = MakeSureHasSuffix(dir, "/")
	if _, ok := fs.Directories[dir]; !ok {
		return &os.PathError{Op: "write", Path: filepath, Err: syscall.ENOENT}

	}
	fs.DirectoryFiles[dir] = append(fs.DirectoryFiles[dir], NewSimpleEntryTimeSize(filepath, false, int64(len(content)), time.Time{}))
	fs.Contents[filepath] = content
	return nil
}

func (fs *MemFS) Mkdir(dirpath string) error {
	fs.mutex.Lock()
	defer func() { fs.mutex.Unlock() }()
	if bs, ok := fs.Contents[dirpath]; ok && bs != nil {
		return &os.PathError{Op: "mkdir", Path: dirpath, Err: syscall.ENOTDIR}
	}
	if !strings.HasPrefix(dirpath, "/") {
		dirpath = "/" + dirpath

	}
	if !strings.HasSuffix(dirpath, "/") {
		dirpath = dirpath + "/"
	}
	dirparts := strings.Split(dirpath, "/")
	if len(dirparts[0]) == 0 {
		dirparts = dirparts[1:]
	}
	if len(dirparts[len(dirparts)-1]) == 0 {
		dirparts = dirparts[:len(dirparts)-1]
	}
	currpath := "/"
	for i := 0; i < len(dirparts); i++ {
		currpath = currpath + dirparts[i] + "/"
		fs.Directories[currpath] = utils.U
	}
	fs.Contents[dirpath] = nil
	return nil
}
