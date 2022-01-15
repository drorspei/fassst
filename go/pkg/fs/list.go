package fs

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"fassst/pkg/utils"
)

func List(fs FileSystem, startPath string, routines int, cont func([]string, *sync.WaitGroup)) *sync.WaitGroup {
	runChan := make(chan utils.Unit, routines)
	var runWG sync.WaitGroup
	var contWG sync.WaitGroup

	runWG.Add(1)
	go lister(fs, startPath, nil, runChan, &runWG, cont, &contWG)

	runWG.Wait()
	return &contWG
}

func lister(
	fs FileSystem, url string, pagination Pagination,
	runChan chan utils.Unit, runWG *sync.WaitGroup,
	cont func([]string, *sync.WaitGroup), contWG *sync.WaitGroup,
) {
	runChan <- utils.U
	defer func() { <-runChan }()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		if strings.Contains(err.Error(), "SlowDown") {
			time.Sleep(time.Second)
			go lister(fs, url, nil, runChan, runWG, cont, contWG)
			return
		}
		panic(fmt.Errorf("%s: %w", url, err))
	}

	if new_pagination != nil {
		runWG.Add(1)
		go lister(fs, url, new_pagination, runChan, runWG, cont, contWG)
	}

	for _, d := range dirs {
		runWG.Add(1)
		go lister(fs, MakeSureHasSuffix(d.Name(), "/"), nil, runChan, runWG, cont, contWG)
	}

	filenames := make([]string, len(files))
	for i := 0; i < len(files); i++ {
		filenames[i] = files[i].Name()
	}

	contWG.Add(1)
	cont(filenames, contWG)
	runWG.Done()
}
