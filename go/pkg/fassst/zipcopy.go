package fassst

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
	"fassst/pkg/utils"
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
			strings.ReplaceAll(uuid.New().String(), "-", "_")[:8],
		)
		dir := path.Dir(prefix)
		if err := targetFs.Mkdir(targetPath + dir); err != nil {
			log.Error("mkdir target", zap.Error(err))
			return
		}
		log.Info("writing archive", zap.String("path", archivePath))
		if _, err := targetFs.WriteFile(archivePath, archiveBuffer, time.Now()); err != nil {
			log.Error("write archive file", zap.Error(err))
			return
		}
	}
}

func ZipCopyPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	List(sourceFs, sourcePath, routines, func(files []pkgfs.FileEntry) {
		buf := make([]byte, 64*1024*1024)

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
			fh, err := zip.FileInfoHeader(file)
			if err != nil {
				log.Error("init zip header from file", zap.String("filename", file.Name()), zap.Error(err))
				continue
			}
			fh.Method = zip.Deflate
			fw, err := zipWriter.CreateHeader(fh)
			if err != nil {
				panic(fmt.Errorf("could not create file entry in archive: %w", err))
			}
			batchNames = append(batchNames, outputFilename)
			content, err := sourceFs.ReadFile(file.Name())
			if err != nil {
				panic(fmt.Errorf("read file: %w", err))
			}

			sz, err := io.CopyBuffer(fw, content, buf)
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

func writeBatch(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, names []string, runChan chan utils.Unit, runWG *sync.WaitGroup, log *zap.Logger) {
	buf := make([]byte, 64*1024*1024)

	runChan <- utils.U
	defer func() {
		<-runChan
		runWG.Done()
	}()

	archiveBuffer := new(bytes.Buffer)
	archiveWriter := bufio.NewWriter(archiveBuffer)
	zipWriter := zip.NewWriter(archiveWriter)
	CloseAndWriteArchive := genArchiveCloser(targetFs, targetPath, archiveBuffer, zipWriter, log)
	var outputNames []string
	for _, filename := range names {
		outputFilename := strings.Replace(filename, sourcePath, "", 1)
		outputNames = append(outputNames, outputFilename)
		fw, err := zipWriter.Create(outputFilename)
		if err != nil {
			log.Error("create file entry in archive", zap.Error(err))
			continue
		}
		content, err := sourceFs.ReadFile(filename)
		if err != nil {
			log.Error("read file", zap.Error(err))
			continue
		}

		_, err = io.CopyBuffer(fw, content, buf)
		if err != nil {
			log.Error("write content to file", zap.Error(err))
			continue
		}
	}
	CloseAndWriteArchive(outputNames)
}

func ZipCopyAcrossPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, listRoutines, archiveRoutines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	var batchNames []string
	var fileSize int64
	resChan := make(chan []pkgfs.FileEntry, listRoutines)
	batchChan := make(chan []string, archiveRoutines)

	var archiveDone, writeDone bool
	go func() {
		runChan := make(chan utils.Unit, archiveRoutines)
		var runWG sync.WaitGroup
		for !archiveDone || len(batchChan) > 0 {
			select {
			case batch := <-batchChan:
				runWG.Add(1)
				go writeBatch(sourceFs, targetFs, sourcePath, targetPath, batch, runChan, &runWG, log)
			default:
				time.Sleep(time.Millisecond)
			}
		}
		runWG.Wait()
		writeDone = true
	}()

	var listDone, transferDone bool
	go func() {
		for !listDone || len(resChan) > 0 {
			select {
			case files := <-resChan:
				sort.Slice(files, func(i, j int) bool {
					return files[i].Name() < files[j].Name()
				})
				for _, file := range files {
					if len(batchNames)+1 > maxBatchCount || (fileSize+file.Size() >= int64(maxBatchSize) && len(batchNames) > 0) {
						batchChan <- batchNames
						fileSize = 0
						batchNames = make([]string, 0)
					}
					batchNames = append(batchNames, file.Name())
					fileSize += file.Size()
				}
			default:
				time.Sleep(time.Millisecond)
			}
		}
		if len(batchNames) > 0 {
			batchChan <- batchNames
		}
		transferDone = true
	}()
	log.Info("listing...")

	List(sourceFs, sourcePath, listRoutines, func(fe []pkgfs.FileEntry) {
		resChan <- fe
	}, log)
	listDone = true
	for !transferDone {
		time.Sleep(time.Millisecond)
	}
	archiveDone = true
	for !writeDone {
		time.Sleep(time.Millisecond)
	}
}
