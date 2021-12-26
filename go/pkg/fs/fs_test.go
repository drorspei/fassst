package fs_test

import (
	"testing"

	ffs "fassst/pkg/fs"
)

func Test_fs(t *testing.T) {
	url, fs, err := ffs.FileSystemByUrl("mock://6:6:10:100@root")
	if err != nil {
		t.Fatalf("get mock fs: %v", err)
	}
	if url != "root" {
		t.Fatalf("mock url: expected=root | actual=%s", url)
	}

}
