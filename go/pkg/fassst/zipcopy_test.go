package fassst_test

import (
	"fassst/pkg/fassst"
	"fmt"
	"testing"
)

func Test_Prefix(t *testing.T) {
	tests := []struct {
		names  []string
		prefix string
	}{
		{
			[]string{
				"abcd",
				"abc",
				"ab",
			},
			"ab",
		},
		{
			[]string{
				"ab",
				"abc",
				"abcd",
			},
			"ab",
		},
		{
			[]string{
				"/foo/goo",
				"/foo/goo/ha",
				"/foo/goo/ba",
			},
			"/foo/goo",
		},
	}

	for ti, tt := range tests {
		t.Run(fmt.Sprint(ti), func(t *testing.T) {
			actual := fassst.CommonPrefix(tt.names)
			if actual != tt.prefix {
				t.Fatalf("Expected: %s, Actual: %s", tt.prefix, actual)
			}
		})
	}
}
