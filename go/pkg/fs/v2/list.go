package fs_v2

import (
	"time"

	"github.com/google/uuid"

	ffs "fassst/pkg/fs"
	"fassst/pkg/utils"
)

func List(fs ffs.FileSystem, startPath string, routines, pageSize int) ([]string, error) {
	drainChan := make(chan []string, routines)
	nameChan := make(chan string, routines*2)
	runChan := make(chan utils.Unit, routines)
	errChan := make(chan error)

	nameMap := make(map[string]utils.Unit, routines)
	var results []string

	uid := uuid.New().String()
	nameChan <- uid
	go lister(fs, startPath, uid, nil, runChan, nameChan, drainChan, errChan)

	var done, eating bool
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
	for !done || len(drainChan) > 0 || eating {
		select {
		case err := <-errChan:
			return nil, err
		case name := <-nameChan:
			if _, ok := nameMap[name]; ok {
				delete(nameMap, name)
			} else {
				nameMap[name] = utils.U
			}
			if len(nameMap) == 0 {
				done = true
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}

	return results, nil
}

func lister(fs ffs.FileSystem, url, name string, pagination ffs.Pagination, runChan chan utils.Unit, nameChan chan string, drainChan chan []string, errChan chan error) {
	runChan <- utils.U
	defer func() { <-runChan }()

	dirs, files, new_pagination, err := fs.ReadDir(url, pagination)
	if err != nil {
		errChan <- err
		return
	}

	if new_pagination != nil {
		uid := uuid.New().String()
		nameChan <- uid
		go lister(fs, url, uid, new_pagination, runChan, nameChan, drainChan, errChan)
	}

	for _, d := range dirs {
		uid := uuid.New().String()
		nameChan <- uid
		go lister(fs, ffs.MakeSureHasSuffix(d.Name(), "/"), uid, nil, runChan, nameChan, drainChan, errChan)
	}

	filenames := make([]string, len(files))
	for i := 0; i < len(files); i++ {
		filenames[i] = files[i].Name()
	}
	drainChan <- filenames

	nameChan <- name
}
