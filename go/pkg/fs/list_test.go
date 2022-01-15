package fs_test

import (
	"fmt"
	"sync"
	"testing"

	ffs "fassst/pkg/fs"
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
			_, fs, err := ffs.FileSystemByUrl(consPath)
			if err != nil {
				t.Fatalf("get mock fs: %v", err)
			}

			resChan := make(chan string, tt.expected)
			wg := ffs.List(fs, "/", tt.routines, func(input []string, contWG *sync.WaitGroup) {
				for _, i := range input {
					resChan <- i
				}
				contWG.Done()
			})
			wg.Wait()
			if len(resChan) != tt.expected {
				t.Fatalf("len results expected=%d, actual=%d", tt.expected, len(resChan))
			}
			close(resChan)
			var res []string
			for r := range resChan {
				res = append(res, r)
			}
			if len(res) != tt.expected {
				t.Fatalf("len results expected=%d, actual=%d", tt.expected, len(resChan))
			}
		})
	}
}

func Benchmark_fs_list(b *testing.B) {
	r := 100
	b.Run(fmt.Sprint(r), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, fs, _ := ffs.FileSystemByUrl("mock://5:5:10:1000@")
			resChan := make(chan string, 3125)
			wg := ffs.List(fs, "/", r, func(input []string, contWG *sync.WaitGroup) {
				for _, i := range input {
					resChan <- i
				}
				contWG.Done()
			})
			wg.Wait()
			if len(resChan) != 3125 {
				b.Fatalf("len results expected=%d, actual=%d", 3125, len(resChan))
			}
		}
	})
}
