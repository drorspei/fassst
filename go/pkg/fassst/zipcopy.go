package fassst

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	pkgfs "fassst/pkg/fs"
)

func ZipCopyPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	wg := List(sourceFs, sourcePath, routines, func(files []string, contWG *sync.WaitGroup) {
		var fileCount int
		var fileSize uint64
		var archiveBuffer *bytes.Buffer
		var archiveWriter *bufio.Writer
		var zipWriter *zip.Writer
		CloseAndWriteArchive := func() {
			err := zipWriter.Close()
			if err != nil {
				panic(fmt.Errorf("close zip writer: %w", err))
			}
			archivePath := fmt.Sprintf("%s%s_archive_%s.zip",
				targetPath,
				time.Now().Format("2006_01_02_15_04_05"),
				uuid.New().String(),
			)
			if err := targetFs.Mkdir(targetPath); err != nil {
				panic(fmt.Errorf("mkdir target: %w", err))
			}
			if err := targetFs.WriteFile(archivePath, archiveBuffer.Bytes()); err != nil {
				panic(fmt.Errorf("write archive file: %w", err))
			}
		}

		for _, filename := range files {
			if fileCount == 0 {
				archiveBuffer = new(bytes.Buffer)
				archiveWriter = bufio.NewWriter(archiveBuffer)
				zipWriter = zip.NewWriter(archiveWriter)
			}
			outputFilename := strings.Replace(filename, sourcePath, "", 1)
			fw, err := zipWriter.Create(outputFilename)
			if err != nil {
				panic(fmt.Errorf("could not create file entry in archive: %w", err))
			}

			content, err := sourceFs.ReadFile(filename)
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
				CloseAndWriteArchive()
			}
		}
		if fileCount > 0 {
			CloseAndWriteArchive()
		}
		contWG.Done()
	}, log)
	wg.Wait()
}

func ZipCopyAcrossPages(sourceFs, targetFs pkgfs.FileSystem, sourcePath, targetPath string, routines int, maxBatchCount, maxBatchSize int, log *zap.Logger) {
	batchChan := make(chan []string, routines)
	wg := List(sourceFs, sourcePath, routines, func(files []string, contWG *sync.WaitGroup) {
		batchChan <- files
		contWG.Done()
	}, log)
	var listingDone bool
	go func() {
		wg.Wait()
		for len(batchChan) > 0 {
			time.Sleep(time.Millisecond)
		}
		listingDone = true
	}()

	var fileCount int
	var fileSize uint64
	var archiveBuffer *bytes.Buffer
	var archiveWriter *bufio.Writer
	var zipWriter *zip.Writer
	CloseAndWriteArchive := func() {
		err := zipWriter.Close()
		if err != nil {
			panic(fmt.Errorf("close zip writer: %w", err))
		}

		archivePath := fmt.Sprintf("%s%s_archive_%s.zip",
			targetPath,
			time.Now().Format("2006_01_02_15_04_05"),
			uuid.New().String(),
		)

		if err := targetFs.Mkdir(targetPath); err != nil {
			panic(fmt.Errorf("mkdir target: %w", err))
		}

		if err := targetFs.WriteFile(archivePath, archiveBuffer.Bytes()); err != nil {
			panic(fmt.Errorf("write archive file: %w", err))
		}
	}
	for !listingDone {
		select {
		case result := <-batchChan:
			for _, filename := range result {
				if fileCount == 0 {
					archiveBuffer = new(bytes.Buffer)
					archiveWriter = bufio.NewWriter(archiveBuffer)
					zipWriter = zip.NewWriter(archiveWriter)
				}
				outputFilename := strings.Replace(filename, sourcePath, "", 1)
				fw, err := zipWriter.Create(outputFilename)
				if err != nil {
					panic(fmt.Errorf("could not create file entry in archive: %w", err))
				}

				content, err := sourceFs.ReadFile(filename)
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

				}
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if fileCount > 0 {
		CloseAndWriteArchive()
	}
}
