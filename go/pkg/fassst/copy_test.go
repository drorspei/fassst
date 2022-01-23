package fassst_test

import (
	"fmt"
	"testing"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"

	"go.uber.org/zap"
)

func Test_fs_copy(t *testing.T) {
	tests := []struct {
		sourcePath       string
		targetPath       string
		routines         int
		expectedContents map[string][]byte
	}{
		{
			sourcePath: "mock://2:2:10:100@",
			targetPath: "mem://foo1/",
			routines:   10,
			expectedContents: map[string][]byte{
				"/foo1/0/":  nil,
				"/foo1/0/0": []byte("0"),
				"/foo1/0/1": []byte("1"),
				"/foo1/1/":  nil,
				"/foo1/1/0": []byte("0"),
				"/foo1/1/1": []byte("1"),
			},
		},
		{
			sourcePath: "mock://3:3:10:100@0/",
			targetPath: "mem://foo2/",
			routines:   10,
			expectedContents: map[string][]byte{
				"/foo2/0/":  nil,
				"/foo2/0/0": []byte("0"),
				"/foo2/0/1": []byte("1"),
				"/foo2/0/2": []byte("2"),
				"/foo2/1/":  nil,
				"/foo2/1/0": []byte("0"),
				"/foo2/1/1": []byte("1"),
				"/foo2/1/2": []byte("2"),
				"/foo2/2/":  nil,
				"/foo2/2/0": []byte("0"),
				"/foo2/2/1": []byte("1"),
				"/foo2/2/2": []byte("2"),
			},
		},
		{
			sourcePath: "mock://4:4:10:100@3/3/3/",
			targetPath: "mem://foo3/",
			routines:   10,
			expectedContents: map[string][]byte{
				"/foo3/":  nil,
				"/foo3/0": []byte("0"),
				"/foo3/1": []byte("1"),
				"/foo3/2": []byte("2"),
				"/foo3/3": []byte("3"),
			},
		},
	}

	for ti, tt := range tests {
		t.Run(fmt.Sprintf("%d) %d:", ti, tt.routines), func(t *testing.T) {
			srcUrl, srcFs, err := pkgfs.FileSystemByUrl(tt.sourcePath)
			if err != nil {
				t.Fatalf("get source fs: %v", err)
			}
			tgtUrl, tgtFs, err := pkgfs.FileSystemByUrl(tt.targetPath)
			if err != nil {
				t.Fatalf("get target fs: %v", err)
			}
			log, _ := zap.NewProduction()
			fst.Copy(srcFs, tgtFs, srcUrl, tgtUrl, tt.routines, log)

			actual := tgtFs.(*pkgfs.MemFS).Contents
			actualCount := len(actual)

			if len(tt.expectedContents) != actualCount {
				t.Fatalf("number of results, expected: %d, actual %d", len(tt.expectedContents), actualCount)
			}
			for k, v := range actual {
				if _, ok := tt.expectedContents[k]; !ok {
					t.Fatalf("missing key %s", k)
				}
				for i, b := range tt.expectedContents[k] {
					if v[i] != b {
						t.Fatalf("data not equal expected:\"%s\" actual:\"%s\"", tt.expectedContents[k], v)
					}
				}
			}

		})
	}
}
