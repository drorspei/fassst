package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"
)

type listOptions struct {
	*options

	source string

	outputFile string
}

func NewListCommand(opts *options) *cobra.Command {
	var listOpts listOptions
	listOpts.options = opts
	command := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "list entries under source",
		Example: "fassst list path/to/source",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			listOpts.source = pkgfs.MakeSureHasSuffix(args[0], "/")
			return listOpts.run()
		},
	}

	command.Flags().StringVarP(&listOpts.outputFile,
		"output-file",
		"o",
		"",
		"name of output file or empty for stdout")

	return command
}

func (o listOptions) run() error {
	url, fs, err := pkgfs.FileSystemByUrl(o.source)
	if err != nil {
		return fmt.Errorf("file system from url: %w", err)
	}
	o.log.Info("listing...")
	resChan := make(chan string, o.maxGoroutines)
	wg := fst.List(fs, url, o.maxGoroutines, func(input []string, contWG *sync.WaitGroup) {
		for _, i := range input {
			resChan <- i
		}
		o.log.Debug("page done")
		contWG.Done()
	}, o.log)
	wg.Wait()
	o.log.Info("collecting...")
	close(resChan)
	var res []string
	for r := range resChan {
		res = append(res, r)
	}
	if len(o.outputFile) > 0 {
		if err := os.WriteFile(o.outputFile, []byte(strings.Join(res, "\n")), 0644); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}
	} else {
		for _, r := range res {
			fmt.Println(r)
		}
		o.log.Info("entries listed", zap.Int("count", len(res)))
	}
	return nil
}
