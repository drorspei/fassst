package fs_v2_test

import (
	"fmt"
	"testing"

	fs0 "fassst/pkg/fs"
	ffs "fassst/pkg/fs/v2"
)

func Test_fs2(t *testing.T) {
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
			_, fs, err := fs0.FileSystemByUrl(consPath)
			if err != nil {
				t.Fatalf("get mock fs: %v", err)
			}

			results, err := ffs.List(fs, "/", tt.routines)
			if err != nil {
				t.Fatalf("list mock fs: %v", err)
			}
			if len(results) != tt.expected {
				fmt.Println(results)
				t.Fatalf("len results expected=%d, actual=%d", tt.expected, len(results))
			}
		})
	}
}

func Benchmark_fs2(b *testing.B) {
	r := 100
	b.Run(fmt.Sprint(r), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, fs, _ := fs0.FileSystemByUrl("mock://5:5:10:1000@")

			res, err := ffs.List(fs, "/", r)

			if err != nil || len(res) != 3125 {
				b.Fatalf("len results expected=%d, actual=%d, err=%v", 3125, len(res), err)
			}
		}
	})
}
