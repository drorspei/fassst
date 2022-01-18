package fassst_test

import (
	"fmt"
	"testing"
	"time"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"

	"go.uber.org/zap"
)

func Test_fs_list(t *testing.T) {
	tests := []struct {
		pathFmt  string
		depth    int
		degree   int
		routines int
		expected int
	}{
		{
			pathFmt:  "mock://%d:%d:1:100@",
			depth:    3,
			degree:   3,
			routines: 2,
			expected: 27,
		},
		{
			pathFmt:  "mock://%d:%d:10:100@",
			depth:    4,
			degree:   4,
			routines: 10,
			expected: 256,
		},
		{
			pathFmt:  "mock://%d:%d:10:200@",
			depth:    5,
			degree:   5,
			routines: 10,
			expected: 3125,
		},
		{
			pathFmt:  "mock://%d:%d:10:500@",
			depth:    5,
			degree:   6,
			routines: 100,
			expected: 7776,
		},
		{
			pathFmt:  "mock://%d:%d:10:1000@",
			depth:    6,
			degree:   5,
			routines: 100,
			expected: 15625,
		},
	}

	for ti, tt := range tests {
		t.Run(fmt.Sprintf("%d) %d,%d x %d:", ti, tt.depth, tt.degree, tt.routines), func(t *testing.T) {
			consPath := fmt.Sprintf(tt.pathFmt, tt.depth, tt.degree)
			_, fs, err := pkgfs.FileSystemByUrl(consPath)
			if err != nil {
				t.Fatalf("get mock fs: %v", err)
			}

			resChan := make(chan string, tt.expected)
			log, _ := zap.NewProduction()

			var res []string
			var done, reading bool
			reading = true
			go func() {
				for !done || len(resChan) > 0 {
					select {
					case r := <-resChan:
						res = append(res, r)

					default:
						time.Sleep(time.Millisecond)
					}
				}
				reading = false
			}()

			fst.List(fs, "/", tt.routines, func(input []pkgfs.FileEntry) {
				for _, i := range input {
					resChan <- i.Name()
				}
			}, log)

			done = true
			for reading {
				time.Sleep(time.Millisecond)
			}

			if len(res) != tt.expected {
				t.Fatalf("len results expected=%d, actual=%d", tt.expected, len(res))
			}
		})
	}
}

func Benchmark_fs_list(b *testing.B) {
	r := 100
	b.Run(fmt.Sprint(r), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, fs, _ := pkgfs.FileSystemByUrl("mock://5:5:10:1000@")
			resChan := make(chan string, 3125)
			log, _ := zap.NewProduction()
			fst.List(fs, "/", r, func(input []pkgfs.FileEntry) {
				for _, i := range input {
					resChan <- i.Name()
				}
			}, log)
			if len(resChan) != 3125 {
				b.Fatalf("len results expected=%d, actual=%d", 3125, len(resChan))
			}
		}
	})
}
