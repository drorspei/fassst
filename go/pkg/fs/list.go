package fs

import (
	"strings"
	"sync"
	"time"

	"fassst/pkg/utils"
)

func List(fs FileSystem, startPath string, routines, pageSize int, cont func(interface{})) ([]string, error) {
	drainChan := make(chan []string, routines)
	runChan := make(chan utils.Unit, routines)
	errChan := make(chan error)
	var runWG sync.WaitGroup

	var results []string

	runWG.Add(1)
	go lister(fs, startPath, nil, runChan, &runWG, drainChan, errChan)

	var eating bool
	go func() {
		var v []string
		for {
			select {
			case err := <-errChan:
				panic(err)
			case v, eating = <-drainChan:
				results = append(results, v...)
				cont(v)
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

func lister(
	fs FileSystem, url string, pagination Pagination,
	runChan chan utils.Unit, runWG *sync.WaitGroup,
	drainChan chan []string, errChan chan error,
) {
	runChan <- utils.U
	defer func() { <-runChan }()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		if strings.Contains(err.Error(), "SlowDown") {
			time.Sleep(time.Second)
			go lister(fs, url, pagination, runChan, runWG, drainChan, errChan)
			return
		}
		errChan <- err
		return
	}

	if new_pagination != nil {
		runWG.Add(1)
		go lister(fs, url, new_pagination, runChan, runWG, drainChan, errChan)
	}

	for _, d := range dirs {
		runWG.Add(1)
		go lister(fs, MakeSureHasSuffix(d.Name(), "/"), nil, runChan, runWG, drainChan, errChan)
	}

	filenames := make([]string, len(files))
	for i := 0; i < len(files); i++ {
		filenames[i] = files[i].Name()
	}
	drainChan <- filenames
	runWG.Done()
}
