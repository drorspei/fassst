package main

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const parallel = 100
const page = 1000

type Unit = struct{}

var U = Unit{}

func main() {
	t0 := time.Now()
	res, err := run("gen/gen_data/", parallel, page)
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

func run(startPath string, routines, pageSize int) ([]string, error) {
	drainChan := make(chan string, routines*pageSize)
	nameChan := make(chan string, routines)
	runChan := make(chan Unit, routines)
	errChan := make(chan error)

	nameMap := make(map[string]Unit, routines)
	var results []string

	uid := uuid.New().String()
	nameChan <- uid
	go foo(startPath, uid, runChan, nameChan, drainChan, errChan)

	go func() {
		for {
			select {
			case v := <-drainChan:
				results = append(results, v)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	var stop bool
	for !stop {
		select {
		case err := <-errChan:
			return nil, err
		case name := <-nameChan:
			if _, ok := nameMap[name]; ok {
				delete(nameMap, name)
			} else {
				nameMap[name] = U
			}
			if len(nameMap) == 0 {
				stop = true
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for len(drainChan) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	return results, nil
}

func foo(url, name string, runChan chan Unit, nameChan, drainChan chan string, errChan chan error) {
	runChan <- U

	ds, fs, last, hasMore, err := Query(url)
	if err != nil {
		errChan <- err
		return
	}
	if hasMore {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(last, uid, runChan, nameChan, drainChan, errChan)
	}
	for _, d := range ds {
		uid := uuid.New().String()
		nameChan <- uid
		go foo(d, uid, runChan, nameChan, drainChan, errChan)
	}
	for _, f := range fs {
		drainChan <- f
	}
	<-runChan
	nameChan <- name
}

func Query(key string) ([]string, []string, string, bool, error) {
	info, err := os.Stat(key)
	var cont bool
	var startName string
	if err != nil || info == nil || !info.IsDir() {
		key, startName = path.Split(key)
		cont = true
	} else if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	dirents, err := os.ReadDir(key)
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("read dir '%s': %w", key, err)
	}
	var files []string
	var dirs []string
	count := 0
	var last string
	for _, d := range dirents {
		if cont && startName >= d.Name() {
			continue
		}
		p := fmt.Sprintf("%s%s", key, d.Name())
		if d.IsDir() {
			p += "/"
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}
		count++
		if count >= page {
			last = p
			break
		}
	}
	return dirs, files, last, count >= page, nil
	
}
