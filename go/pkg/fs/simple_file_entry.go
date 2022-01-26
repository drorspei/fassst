package fs

import (
	"io/fs"
	"os"
	"time"
)

type SimpleFileEntry struct {
	Path     string
	Bytes    int64
	Modified time.Time
	FileMode fs.FileMode
	Dir      bool
	System   interface{}
}

func (f SimpleFileEntry) Name() string {
	return f.Path
}
func (f SimpleFileEntry) Size() int64 {
	return f.Bytes
}
func (f SimpleFileEntry) ModTime() time.Time {
	return f.Modified
}
func (f SimpleFileEntry) Mode() fs.FileMode {
	return f.FileMode
}
func (f SimpleFileEntry) IsDir() bool {
	return f.Dir
}
func (f SimpleFileEntry) Sys() interface{} {
	return f.System
}

func NewSimpleEntry(path string, isDir bool) SimpleFileEntry {
	var fm fs.FileMode
	var size int64
	fm = 0755
	if isDir {
		fm = fm | fs.ModeDir
	} else {
		size++
	}

	return SimpleFileEntry{
		Path:     path,
		Dir:      isDir,
		FileMode: fm,
		Bytes:    size,
		Modified: time.Time{},
		System:   nil,
	}
}

func NewSimpleEntryTimeSize(path string, isDir bool, size int64, modTime time.Time) SimpleFileEntry {
	var fm fs.FileMode
	fm = 0755
	if isDir {
		fm = fm | fs.ModeDir
	}

	return SimpleFileEntry{
		Path:     path,
		Dir:      isDir,
		FileMode: fm,
		Bytes:    size,
		Modified: modTime,
		System:   nil,
	}
}

func RenameFileEntry(fi os.FileInfo, name string) SimpleFileEntry {
	return SimpleFileEntry{
		Path:     name,
		Dir:      fi.IsDir(),
		FileMode: fi.Mode(),
		Bytes:    fi.Size(),
		Modified: fi.ModTime(),
		System:   fi.Sys(),
	}
}
