package fs

import (
	"fmt"
	"os"
	"time"
)

type LocalFS struct{}

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
			dirs = append(dirs, RenameFileEntry(info, MakeSureHasSuffix(key+de.Name(), "/")))
		} else {
			files = append(files, RenameFileEntry(info, key+de.Name()))
		}
	}
	return dirs, files, nil, nil
}

func (fs LocalFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (fs LocalFS) WriteFile(path string, content []byte, modTime time.Time) error {
	if err := os.WriteFile(path, content, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	if err := os.Chtimes(path, time.Now(), modTime); err != nil {
		return fmt.Errorf("update time: %w", err)
	}
	return nil
}

func (fs LocalFS) Mkdir(path string) error {
	return os.MkdirAll(path, 0644)
}
