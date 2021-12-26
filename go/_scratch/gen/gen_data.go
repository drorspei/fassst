//go:build ignore

package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
)

const years = 2
const months = 4
const days = 6
const minFiles = 2000
const maxFiles = 10000

func main() {
	hasher := md5.New()
	var paths []string
	for year := 1; year <= years; year++ {
		io.WriteString(hasher, fmt.Sprintf("%d", year))
		for month := 1; month <= months; month++ {
			io.WriteString(hasher, fmt.Sprintf("%d", month))
			for day := 1; day <= days; day++ {
				fmt.Println("year", year, "month", month, "day", day)
				io.WriteString(hasher, fmt.Sprintf("%d", day))
				fileCount := rand.Intn(maxFiles-minFiles) + minFiles
				dirpath := fmt.Sprintf("./gen_data/%04d/%02d/%02d", year, month, day)
				err := os.MkdirAll(dirpath, 0644)
				if err != nil {
					panic(err)
				}
				for fileId := 1; fileId <= fileCount; fileId++ {
					io.WriteString(hasher, fmt.Sprintf("%d", fileId))
					filename := fmt.Sprintf("%x", hasher.Sum(nil))
					filepath := fmt.Sprintf("%s/%s", dirpath, filename)
					paths = append(paths, filepath)
					err = os.WriteFile(filepath, []byte(filepath), 0644)
					if err != nil {
						panic(err)
					}
					if fileId%minFiles == 0 {
						fmt.Println("progress", fileId)
					}
				}
				fmt.Println("items", fileCount)
			}
		}
	}
	os.WriteFile("./gen_data/toc.txt", []byte(strings.Join(paths, "\n")), 0644)
}
