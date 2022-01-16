package fassst

import (
	"path"
	"strings"
	"sync"

	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
)

func Copy(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, log *zap.Logger) {
	wg := List(sourceFs, sourcePath, routines, func(files []string, contWG *sync.WaitGroup) {
		for _, filename := range files {
			outputFilename := strings.Replace(filename, sourcePath, targetPath, 1)
			pathBase := path.Base(outputFilename)
			pathDir := outputFilename[:len(outputFilename)-len(pathBase)]
			targetFs.Mkdir(pathDir)
			content, err := sourceFs.ReadFile(filename)
			if err != nil {
				log.Error("read file", zap.Error(err))
				continue
			}
			log.Debug("writing file", zap.String("filename", outputFilename))
			targetFs.WriteFile(outputFilename, content)
			log.Debug("wrote file", zap.String("filename", outputFilename), zap.Int("size", len(content)))
		}
		contWG.Done()
		log.Debug("list page done")
	}, log)
	wg.Wait()
}
