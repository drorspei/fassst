package fs_test

import (
	"fmt"
	"testing"

	ffs "fassst/pkg/fs"
)

func Test_Mock_Fs_Sanity(t *testing.T) {
	tests := []struct {
		parseUrl      string
		expectedUrl   string
		expectedFiles map[string][]string
	}{
		{
			parseUrl:    "mock://2:3:1000:1000@/",
			expectedUrl: "/",
			expectedFiles: map[string][]string{
				"/0/": {"0/0", "0/1", "0/2"},
				"/1/": {"1/0", "1/1", "1/2"},
				"/2/": {"2/0", "2/1", "2/2"},
			},
		},
		{
			parseUrl:    "mock://3:2:1000:1000@/0",
			expectedUrl: "/0",
			expectedFiles: map[string][]string{
				"/0/0/": {"0/0/0", "0/0/1"},
				"/0/1/": {"0/1/0", "0/1/1"},
			},
		},
	}
	for ti, tt := range tests {
		t.Run(fmt.Sprint(ti), func(t *testing.T) {
			url, fs, err := ffs.FileSystemByUrl(tt.parseUrl)
			if err != nil {
				t.Fatalf("filesystem from url: %v", err)
			}
			if tt.expectedUrl != url {
				t.Fatalf("urls mismatch, expected %s, got %s", tt.expectedUrl, url)
			}
			for edir, efiles := range tt.expectedFiles {
				dirs, files, _, err := fs.ReadDir(edir, nil)
				if err != nil {
					t.Fatalf("read dir: %v", err)
				}
				if len(dirs) != 0 {
					t.Fatalf("unexpected dirs under %s", edir)
				}
				if len(efiles) != len(files) {
					t.Fatalf("files count not the same, expected %d, got %d", len(efiles), len(files))
				}
				for _, f := range files {
					var found bool
					for i := 0; i < len(efiles); i++ {
						if efiles[i] == f.Name() {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("missing file %s", f.Name())
					}
				}
			}
		})
	}
}

func Test_Mock_Fs_Pagination(t *testing.T) {
	var testUrl string
	var testDir string

	testUrl = "mock://3:3:2:1000@"
	testDir = "2/1/"

	_, fs, err := ffs.FileSystemByUrl(testUrl)
	if err != nil {
		t.Fatalf("filesystem from url: %v", err)
	}
	dirs, files, p, err := fs.ReadDir(testDir, nil)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(dirs) != 0 {
		t.Fatalf("unexpected dirs under %s", testDir)
	}
	if len(files) != 2 {
		t.Fatalf("incorrect amount of files %d expected %d", len(files), 2)
	}
	dirs, files, p2, err := fs.ReadDir(testDir, p)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(dirs) != 0 {
		t.Fatalf("unexpected dirs under %s", testDir)
	}
	if len(files) != 1 {
		t.Fatalf("incorrect amount of files %d expected %d", len(files), 1)
	}
	if p2 != nil {
		t.Fatalf("pagination was %v should be nil", p2)
	}
}

// TODO: What? How?
// func Test_Mock_Fs_Concurrency(t *testing.T) {
// 	tests := []struct {
// 		url      string
// 		routines int
// 	}{
// 		{
// 			url:      "mock://2:3:1000:2@",
// 			routines: 1,
// 		},
// 	}
// 	for ti, tt := range tests {
// 		t.Run(fmt.Sprint(ti), func(t *testing.T) {
// 			url, fs, err := ffs.FileSystemByUrl(tt.url)
// 			if err != nil {
// 				t.Fatalf("filesystem from url: %v", err)
// 			}
// 			dirs, files, _, err := fs.ReadDir(url, nil)
// 			if err != nil {
// 				t.Fatalf("read dir: %v", err)
// 			}
// 			fmt.Println(dirs)
// 			fmt.Println(files)
// 			for _, d := range dirs {
// 				go func() {
// 					subdirs, subfiles, _, err := fs.ReadDir(d.Name(), nil)
// 					if err != nil {
// 						if !strings.Contains(err.Error(), "SlowDown") {
// 							t.Fatalf("read dir: %v", err)

// 						}
// 					}
// 					fmt.Println(subdirs)
// 					fmt.Println(subfiles)
// 				}()
// 			}

// 		})
// 	}
// }
