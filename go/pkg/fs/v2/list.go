package fs_v2

import (
	"strings"
	"sync"
	"time"

	fs0 "fassst/pkg/fs"
	"fassst/pkg/utils"
)

func List(fs fs0.FileSystem, startPath string, routines int) ([]string, error) {
	drainChan := make(chan []string, routines)
	runChan := make(chan utils.Unit, routines)
	var runWG sync.WaitGroup
	var results []string

	runWG.Add(1)
	go lister(fs, startPath, nil, runChan, &runWG, drainChan)

	var eating bool
	go func() {
		var v []string
		for {
			select {
			case v, eating = <-drainChan:
				results = append(results, v...)
				eating = false
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	runWG.Wait()
	for len(drainChan) > 0 || eating {
		time.Sleep(time.Millisecond)
	}

	return results, nil
}

func lister(fs fs0.FileSystem, url string, pagination fs0.Pagination, runChan chan utils.Unit, runWG *sync.WaitGroup, drainChan chan []string) {
	runChan <- utils.U
	defer func() { <-runChan }()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		if strings.Contains(err.Error(), "SlowDown") {
			time.Sleep(time.Second)
			go lister(fs, url, new_pagination, runChan, runWG, drainChan)
			return
		}
		panic(err)
	}

	if new_pagination != nil {
		runWG.Add(1)
		go lister(fs, url, new_pagination, runChan, runWG, drainChan)
	}

	for _, d := range dirs {
		runWG.Add(1)
		go lister(fs, fs0.MakeSureHasSuffix(d.Name(), "/"), nil, runChan, runWG, drainChan)
	}

	filenames := make([]string, len(files))
	for i := 0; i < len(files); i++ {
		filenames[i] = files[i].Name()
	}
	drainChan <- filenames
	runWG.Done()
}
