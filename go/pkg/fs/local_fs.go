package fs

import (
	"fmt"
	"os"
)

type LocalFS struct{}

type SimpleFileEntry struct {
	Path  string
	Bytes int64
}

func (f SimpleFileEntry) Name() string {
	return f.Path
}
func (f SimpleFileEntry) Size() int64 {
	return f.Bytes
}

func (fs LocalFS) ReadDir(key string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	dirents, err := os.ReadDir(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read dir: %w", err)
	}
	var dirs []DirEntry
	var files []FileEntry
	for _, de := range dirents {
		info, err := de.Info()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("file info: %w", err)
		}
		if de.IsDir() {
			dirs = append(dirs, SimpleFileEntry{MakeSureHasSuffix(key+de.Name(), "/"), info.Size()})
		} else {
			files = append(files, SimpleFileEntry{key + de.Name(), info.Size()})
		}
	}
	return dirs, files, nil, nil
}

func (fs LocalFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (fs LocalFS) WriteFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0644)
}

func (fs LocalFS) Mkdir(path string) error {
	return os.MkdirAll(path, 0644)
}
