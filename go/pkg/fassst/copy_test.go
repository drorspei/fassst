package fassst_test

import (
	"fmt"
	"testing"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"
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
				"mem://foo1/0/":  nil,
				"mem://foo1/0/0": []byte("0"),
				"mem://foo1/0/1": []byte("1"),
				"mem://foo1/1/":  nil,
				"mem://foo1/1/0": []byte("0"),
				"mem://foo1/1/1": []byte("1"),
			},
		},
		{
			sourcePath: "mock://3:3:10:100@0/",
			targetPath: "mem://foo2/",
			routines:   10,
			expectedContents: map[string][]byte{
				"mem://foo2/0/":  nil,
				"mem://foo2/0/0": []byte("0"),
				"mem://foo2/0/1": []byte("1"),
				"mem://foo2/0/2": []byte("2"),
				"mem://foo2/1/":  nil,
				"mem://foo2/1/0": []byte("0"),
				"mem://foo2/1/1": []byte("1"),
				"mem://foo2/1/2": []byte("2"),
				"mem://foo2/2/":  nil,
				"mem://foo2/2/0": []byte("0"),
				"mem://foo2/2/1": []byte("1"),
				"mem://foo2/2/2": []byte("2"),
			},
		},
		{
			sourcePath: "mock://4:4:10:100@3/3/3/",
			targetPath: "mem://foo3/",
			routines:   10,
			expectedContents: map[string][]byte{
				"mem://foo3/":  nil,
				"mem://foo3/0": []byte("0"),
				"mem://foo3/1": []byte("1"),
				"mem://foo3/2": []byte("2"),
				"mem://foo3/3": []byte("3"),
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

			fst.Copy(srcFs, tgtFs, srcUrl, tgtUrl, tt.routines)

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

// func Benchmark_fs_copy(b *testing.B) {
// 	r := 100
// 	b.Run(fmt.Sprint(r), func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			_, fs, _ := pkgfs.FileSystemByUrl("mock://5:5:10:1000@")
// 			resChan := make(chan string, 3125)
// 			wg := fst.List(fs, "/", r, func(input []string, contWG *sync.WaitGroup) {
// 				for _, i := range input {
// 					resChan <- i
// 				}
// 				contWG.Done()
// 			})
// 			wg.Wait()
// 			if len(resChan) != 3125 {
// 				b.Fatalf("len results expected=%d, actual=%d", 3125, len(resChan))
// 			}
// 		}
// 	})
// }
