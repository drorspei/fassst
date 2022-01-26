package fs

import (
	"fmt"
	"io"
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

func (fs LocalFS) ReadFile(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (fs LocalFS) WriteFile(path string, content io.Reader, modTime time.Time) (int, error) {
	buf := make([]byte, 64*1024*1024)

	f, err := os.Create(path)

	if err != nil {
		return 0, fmt.Errorf("write file: %w", err)
	}

	n, err := io.CopyBuffer(f, content, buf)
	if err != nil {
		return int(n), fmt.Errorf("copy file: %w", err)
	}

	if err := os.Chtimes(path, time.Now(), modTime); err != nil {
		return int(n), fmt.Errorf("update time: %w", err)
	}

	return int(n), nil
}

func (fs LocalFS) Mkdir(path string) error {
	return os.MkdirAll(path, 0755)
}
