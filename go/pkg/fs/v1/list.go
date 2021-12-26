package fs_v1

import (
	"sync"
	"time"

	ffs "fassst/pkg/fs"
	"fassst/pkg/utils"
)

func List(fs ffs.FileSystem, startPath string, routines, pageSize int) ([]string, error) {
	drainChan := make(chan string, routines*pageSize)
	runChan := make(chan utils.Unit, routines)
	errChan := make(chan error)
	var runWG sync.WaitGroup

	runWG.Add(1)
	go lister(fs, startPath, nil, runChan, &runWG, drainChan, errChan)

	var results []string
	var eating bool
	go func() {
		var v string
		for {
			select {
			case v, eating = <-drainChan:
				results = append(results, v)
				eating = false
			case err := <-errChan:
				panic(err)
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
	fs ffs.FileSystem, url string, pagination ffs.Pagination,
	runChan chan utils.Unit, runWG *sync.WaitGroup,
	drainChan chan string, errChan chan error,
) {
	runChan <- utils.U
	defer func() {
		<-runChan
		runWG.Done()
	}()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		errChan <- err
		return
	}

	if new_pagination != nil {
		runWG.Add(1)
		go lister(fs, url, new_pagination, runChan, runWG, drainChan, errChan)
	}

	for _, d := range dirs {
		runWG.Add(1)
		go lister(fs, ffs.MakeSureHasSuffix(d.Name(), "/"), nil, runChan, runWG, drainChan, errChan)
	}

	for _, f := range files {
		drainChan <- f.Name()
	}
}
