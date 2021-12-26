package fs

import (
	"fmt"
	"os"
)

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
