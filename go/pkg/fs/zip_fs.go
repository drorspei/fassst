package fs

import "fmt"

type ZipFS struct {
	FS             FileSystem
	Directory      string
	BatchFileCount int
	BatchSize      int
}

func NewZipFS(fs FileSystem, path string, batchCount, batchSize int) ZipFS {
	return ZipFS{
		FS:             fs,
		Directory:      path,
		BatchFileCount: batchCount,
		BatchSize:      batchSize,
	}
}

func (z ZipFS) ReadDir(string, Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	return nil, nil, nil, fmt.Errorf("zip fs read dir not implemented")
}

func (z ZipFS) ReadFile(path string) ([]byte, error) {
	return nil, fmt.Errorf("zip fs read file not implemented")
}

func (z ZipFS) WriteFile(path string, content []byte) error {
	return fmt.Errorf("zip fs write file not implemented")
}

func (z ZipFS) Mkdir(path string) error {
	return fmt.Errorf("zip fs mkdir not implemented")

}
