package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	ffs "fassst/pkg/fs"
)

const page = 1000

var parallel = 30

func main() {
	if len(os.Args) > 2 {
		var err error
		if parallel, err = strconv.Atoi(os.Args[2]); err != nil {
			panic(err)
		}
	}

	url := ffs.MakeSureHasSuffix(os.Args[1], "/")

	url, fs, err := ffs.FileSystemByUrl(url)
	if err != nil {
		panic(err)
	}

	t0 := time.Now()
	res, err := ffs.List(fs, url, parallel)
	t1 := time.Now()
	if err != nil {
		panic(err)
	}
	os.WriteFile("toc2.txt", []byte(strings.Join(res, "\n")), 0644)
	t2 := time.Now()
	fmt.Println("Done :)")
	fmt.Println("run", t1.Sub(t0))
	fmt.Println("save", t2.Sub(t1))
}
