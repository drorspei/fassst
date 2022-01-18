package fassst

import (
	"path"
	"strings"

	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
)

func Copy(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, log *zap.Logger) {
	List(sourceFs, sourcePath, routines, func(files []pkgfs.FileEntry) {
		for _, file := range files {
			outputFilename := strings.Replace(file.Name(), sourcePath, targetPath, 1)
			pathBase := path.Base(outputFilename)
			pathDir := outputFilename[:len(outputFilename)-len(pathBase)]
			targetFs.Mkdir(pathDir)
			log.Debug("read source file", zap.String("filename", file.Name()))
			content, err := sourceFs.ReadFile(file.Name())
			if err != nil {
				log.Error("read file", zap.Error(err))
				continue
			}
			log.Debug("make target dir", zap.String("dir", pathDir))
			targetFs.Mkdir(pathDir)
			log.Debug("write target file", zap.String("filename", outputFilename))
			targetFs.WriteFile(outputFilename, content)
			log.Debug("wrote target file", zap.String("filename", outputFilename), zap.Int("size", len(content)))
		}
		log.Debug("list page done")
	}, log)
}
