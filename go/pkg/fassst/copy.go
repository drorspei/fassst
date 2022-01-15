package fassst

import (
	"fmt"
	"path"
	"strings"
	"sync"

	pkgfs "fassst/pkg/fs"
)

func Copy(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int) {
	dirs := make(map[string]struct{})
	wg := List(sourceFs, sourcePath, routines, func(files []string, contWG *sync.WaitGroup) {
		for _, filename := range files {
			outputFilename := strings.Replace(filename, sourcePath, targetPath, 1)
			pathBase := path.Base(outputFilename)
			pathDir := outputFilename[:len(outputFilename)-len(pathBase)]
			if _, ok := dirs[pathDir]; !ok {
				dirs[pathDir] = struct{}{}
				targetFs.Mkdir(pathDir)
			}
			content, err := sourceFs.ReadFile(filename)
			if err != nil {
				panic(fmt.Errorf("read file: %w", err))
			}
			targetFs.WriteFile(outputFilename, content)
		}
		contWG.Done()
	})
	wg.Wait()
}
