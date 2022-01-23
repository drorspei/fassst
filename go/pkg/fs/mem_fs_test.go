package fs_test

import (
	"testing"
	"time"

	ffs "fassst/pkg/fs"
)

func Test_Mem_Fs_Sanity(t *testing.T) {

	_, fs, err := ffs.FileSystemByUrl("mem://")
	if err != nil {
		t.Fatalf("filesystem from url: %v", err)
	}
	if err = fs.WriteFile("foo", []byte("foo"), time.Time{}); err != nil {
		t.Fatalf("write file: %v", err)
	}
	bs, err := fs.ReadFile("/foo")
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(bs) != "foo" {
		t.Fatalf("wrong content: %s expected %s", string(bs), "foo")
	}

	if err = fs.Mkdir("goo/gaa"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err = fs.Mkdir("goo/zaa"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err = fs.WriteFile("goo/gaa/boo", []byte("1"), time.Time{}); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err = fs.WriteFile("goo/zaa/zoo", []byte("2"), time.Time{}); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err = fs.WriteFile("goo/zaa/zee", []byte("3"), time.Time{}); err != nil {
		t.Fatalf("write file: %v", err)
	}

	dirs, files, _, err := fs.ReadDir("/", nil)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	if len(dirs) != 1 {
		t.Fatalf("'/' should have 1 subdir 'goo' found: %v", dirs)
	}

	if len(files) != 1 {
		t.Fatalf("'/' should have 1 file 'foo' found: %v", files)
	}

	dirs, files, _, err = fs.ReadDir("/goo", nil)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	if len(dirs) != 2 {
		t.Fatalf("'/' should have 2 subdirs 'gaa' amd 'zaa' found: %v", dirs)
	}
	if len(files) != 0 {
		t.Fatalf("'/' should have 0 files, found: %v", files)
	}

	dirs, files, _, err = fs.ReadDir("/goo/gaa/", nil)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	if len(dirs) != 0 {
		t.Fatalf("'/' should have 0 subdirs, found: %v", dirs)
	}
	if len(files) != 1 {
		t.Fatalf("'/' should have 1 file 'boo', found: %v", files)
	}

	dirs, files, _, err = fs.ReadDir("/goo/zaa/", nil)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	if len(dirs) != 0 {
		t.Fatalf("'/' should have 0 subdirs, found: %v", dirs)
	}
	if len(files) != 2 {
		t.Fatalf("'/' should have 2 files 'zoo' and 'zee', found: %v", files)
	}

	bs, err = fs.ReadFile("/goo/gaa/boo")
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(bs) != "1" {
		t.Fatalf("wrong content: %s expected %s", string(bs), "1")
	}

	bs, err = fs.ReadFile("/goo/zaa/zoo")
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(bs) != "2" {
		t.Fatalf("wrong content: %s expected %s", string(bs), "2")
	}

	bs, err = fs.ReadFile("/goo/zaa/zee")
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(bs) != "3" {
		t.Fatalf("wrong content: %s expected %s", string(bs), "3")
	}
}
