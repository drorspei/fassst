package fassst

import (
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
	"fassst/pkg/utils"
)

const Version = "v0.5.1"

func List(fs pkgfs.FileSystem, startPath string, routines int, cont func([]pkgfs.FileEntry), log *zap.Logger) {
	runChan := make(chan utils.Unit, routines)
	var runWG sync.WaitGroup
	var contWG sync.WaitGroup

	runWG.Add(1)
	go lister(fs, startPath, nil, runChan, &runWG, cont, &contWG, log)

	runWG.Wait()
}

func lister(
	fs pkgfs.FileSystem, url string, pagination pkgfs.Pagination,
	runChan chan utils.Unit, runWG *sync.WaitGroup,
	cont func([]pkgfs.FileEntry), contWG *sync.WaitGroup,
	log *zap.Logger,
) {
	runChan <- utils.U
	defer func() { <-runChan }()

	log.Debug("starting lister goroutine", zap.String("url", url))
	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		if strings.Contains(err.Error(), "SlowDown") {
			log.Info("slowdown error, waiting", zap.Error(err))
			time.Sleep(time.Second)
			go lister(fs, url, nil, runChan, runWG, cont, contWG, log)
			return
		}
		log.Error("failed to read dir", zap.String("url", url), zap.Error(err))
		runWG.Done()
		return
	}

	if new_pagination != nil {
		runWG.Add(1)
		go lister(fs, url, new_pagination, runChan, runWG, cont, contWG, log)
	}

	for _, d := range dirs {
		runWG.Add(1)
		go lister(fs, pkgfs.MakeSureHasSuffix(d.Name(), "/"), nil, runChan, runWG, cont, contWG, log)
	}

	contWG.Add(1)
	cont(files)
	runWG.Done()
}
