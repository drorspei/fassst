package fassst

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
)

func CommonPrefix(names []string) string {
	if len(names) == 0 {
		return ""
	}
	var prefix string
	for i := 0; i < len(names); i++ {
		current := names[i]
		if len(current) == 0 {
			continue
		}
		if len(prefix) == 0 {
			prefix = names[i]
		}
		for j := 0; j < len(current) && j < len(prefix); j++ {
			if prefix[j] != current[j] {
				if j == 0 {
					return ""
				}
				prefix = prefix[:j]
				break
			}
		}
		if len(current) < len(prefix) {
			prefix = prefix[:len(current)]
		}
	}
	return prefix
}

func genArchiveCloser(targetFs pkgfs.FileSystem, targetPath string, archiveBuffer *bytes.Buffer, zipWriter *zip.Writer, log *zap.Logger) func([]string) {
	return func(names []string) {
		err := zipWriter.Close()
		if err != nil {
			log.Error("close zip writer", zap.Error(err))
			return
		}

		prefix := CommonPrefix(names)
		archivePath := fmt.Sprintf("%s%s_%s_%s.zip",
			targetPath,
			prefix,
			time.Now().Format("20060102_150405"),
			strings.ReplaceAll(uuid.New().String(), "-", "_"),
		)
		dir := path.Dir(prefix)
		fmt.Println(prefix, dir)
		if err := targetFs.Mkdir(targetPath + dir); err != nil {
			log.Error("mkdir target", zap.Error(err))
			return
		}
		log.Info("writing archive", zap.String("path", archivePath))
		if err := targetFs.WriteFile(archivePath, archiveBuffer.Bytes()); err != nil {
			log.Error("write archive file", zap.Error(err))
			return
		}
	}
}

func ZipCopyPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	List(sourceFs, sourcePath, routines, func(files []pkgfs.FileEntry) {
		var batchNames []string
		var fileSize uint64
		var archiveBuffer *bytes.Buffer
		var archiveWriter *bufio.Writer
		var zipWriter *zip.Writer
		var CloseAndWriteArchive func([]string)

		for _, file := range files {
			if len(batchNames) == 0 {
				archiveBuffer = new(bytes.Buffer)
				archiveWriter = bufio.NewWriter(archiveBuffer)
				zipWriter = zip.NewWriter(archiveWriter)
				CloseAndWriteArchive = genArchiveCloser(targetFs, targetPath, archiveBuffer, zipWriter, log)
			}
			outputFilename := strings.Replace(file.Name(), sourcePath, "", 1)
			fw, err := zipWriter.Create(outputFilename)
			if err != nil {
				panic(fmt.Errorf("could not create file entry in archive: %w", err))
			}
			batchNames = append(batchNames, outputFilename)
			content, err := sourceFs.ReadFile(file.Name())
			if err != nil {
				panic(fmt.Errorf("read file: %w", err))
			}

			sz, err := fw.Write(content)
			if err != nil {
				panic(fmt.Errorf("write content to file: %w", err))
			}

			fileSize += uint64(sz)
			if len(batchNames) >= maxBatchCount || fileSize >= uint64(maxBatchSize)*1024*1024 {
				fileSize = 0
				CloseAndWriteArchive(batchNames)
				batchNames = make([]string, 0)
			}
		}
		if len(batchNames) > 0 {
			CloseAndWriteArchive(batchNames)
		}
	}, log)
}

func ZipCopyAcrossPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	var fileCount int
	var fileSize uint64
	var archiveBuffer *bytes.Buffer
	var archiveWriter *bufio.Writer
	var zipWriter *zip.Writer
	CloseAndWriteArchive := func(names []string) {
		err := zipWriter.Close()
		if err != nil {
			panic(fmt.Errorf("close zip writer: %w", err))
		}

		archivePath := fmt.Sprintf("%s%s_%s_%s.zip",
			targetPath,
			strings.ReplaceAll(CommonPrefix(names), "/", "@"),
			time.Now().Format("20060102_150405"),
			strings.ReplaceAll(uuid.New().String(), "-", "_"),
		)

		if err := targetFs.Mkdir(targetPath); err != nil {
			panic(fmt.Errorf("mkdir target: %w", err))
		}

		if err := targetFs.WriteFile(archivePath, archiveBuffer.Bytes()); err != nil {
			panic(fmt.Errorf("write archive file: %w", err))
		}
	}

	batchChan := make(chan []pkgfs.FileEntry, routines)
	var done, writing bool
	writing = true
	go func() {
		for !done || len(batchChan) > 0 {
			select {
			case result := <-batchChan:
				for _, file := range result {
					if fileCount == 0 {
						archiveBuffer = new(bytes.Buffer)
						archiveWriter = bufio.NewWriter(archiveBuffer)
						zipWriter = zip.NewWriter(archiveWriter)
					}
					outputFilename := strings.Replace(file.Name(), sourcePath, "", 1)
					fw, err := zipWriter.Create(outputFilename)
					if err != nil {
						panic(fmt.Errorf("could not create file entry in archive: %w", err))
					}

					content, err := sourceFs.ReadFile(file.Name())
					if err != nil {
						panic(fmt.Errorf("read file: %w", err))
					}

					sz, err := fw.Write(content)
					if err != nil {
						panic(fmt.Errorf("write content to file: %w", err))
					}

					fileSize += uint64(sz)
					fileCount++
					if fileCount >= maxBatchCount || fileSize >= uint64(maxBatchSize)*1024*1024 {
						fileCount = 0
						fileSize = 0
						CloseAndWriteArchive(nil)
					}
				}
			default:
				time.Sleep(time.Millisecond)
			}
		}
		if fileCount > 0 {
			CloseAndWriteArchive(nil)
		}
		writing = false
	}()

	List(sourceFs, sourcePath, routines, func(files []pkgfs.FileEntry) {
		batchChan <- files
	}, log)

	for writing {
		time.Sleep(time.Millisecond)
	}
}

// func ZipCopyAcrossPages2(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, listRoutines, writeRoutines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
// 	var batchFiles []string
// 	var fileSize uint64
// 	var archiveBuffer *bytes.Buffer
// 	var archiveWriter *bufio.Writer
// 	var zipWriter *zip.Writer
// 	resChan := make(chan []pkgfs.FileEntry, listRoutines)
// 	var done, complete bool
// 	go func() {
// 		for !done || len(resChan) > 0 {
// 			select {
// 			case files := <-resChan:
// 				sort.Slice(files, func(i, j int) bool {
// 					return files[i].Name() < files[j].Name()
// 				})

// 			default:
// 				time.Sleep(time.Millisecond)
// 			}
// 		}
// 		complete = true
// 	}()
// }
