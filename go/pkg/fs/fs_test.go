package fs_test

import (
	"fmt"
	"testing"

	ffs "fassst/pkg/fs"
)

func Test_fs(t *testing.T) {
	tests := []struct {
		pathFmt  string
		depth    int
		degree   int
		routines int
		expected int
	}{
		{
			pathFmt:  "mock://%d:%d:10:100000@",
			depth:    4,
			degree:   4,
			routines: 10,
			expected: 4 * 4 * 4 * 4,
		},
		{
			pathFmt:  "mock://%d:%d:10:100000@",
			depth:    5,
			degree:   5,
			routines: 10,
			expected: 5 * 5 * 5 * 5 * 5,
		},
		{
			pathFmt:  "mock://%d:%d:10:100000@",
			depth:    5,
			degree:   6,
			routines: 10,
			expected: 6 * 6 * 6 * 6 * 6,
		},
		{
			pathFmt:  "mock://%d:%d:10:100000@",
			depth:    6,
			degree:   5,
			routines: 10,
			expected: 5 * 5 * 5 * 5 * 5 * 5,
		},
	}

	for ti, tt := range tests {
		t.Run(fmt.Sprintf("%d) %d,%d x %d:", ti, tt.depth, tt.degree, tt.routines), func(t *testing.T) {
			consPath := fmt.Sprintf(tt.pathFmt, tt.depth, tt.degree)
			_, fs, err := ffs.FileSystemByUrl(consPath)
			if err != nil {
				t.Fatalf("get mock fs: %v", err)
			}
			mfs := fs.(ffs.MockKTreeFS)
			mfs.CallDelayMilis = 100
			results, err := ffs.List(fs, "/", tt.routines, 100)
			if err != nil {
				t.Fatalf("list mock fs: %v", err)
			}
			if len(results) != tt.expected {
				t.Fatalf("len results expected=%d, actual=%d", tt.expected, len(results))
			}
		})
	}
}

func Benchmark_fs(b *testing.B) {
	for r := 10; r <= 1010; r += 100 {
		b.Run(fmt.Sprint(r), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, fs, _ := ffs.FileSystemByUrl("mock://5:5:10:100000@")
				mfs := fs.(ffs.MockKTreeFS)
				mfs.CallDelayMilis = 100
				ffs.List(fs, "/", r, 100)
			}
		})
	}
}
