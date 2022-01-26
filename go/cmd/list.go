package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

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
	resChan := make(chan string, o.listingGoroutines)
	var res []string
	var done, reading bool
	reading = true
	go func() {
		for !done || len(resChan) > 0 {
			select {
			case r := <-resChan:
				res = append(res, r)

			default:
				time.Sleep(time.Millisecond)
			}
		}
		reading = false
	}()

	fst.List(fs, url, o.listingGoroutines, func(input []pkgfs.FileEntry) {
		for _, i := range input {
			resChan <- i.Name()
		}
		o.log.Debug("page done")

	}, o.log)

	done = true

	for reading {
		time.Sleep(time.Millisecond)
	}
	o.log.Info("collecting...")

	if len(o.outputFile) > 0 {
		o.log.Info("writing output file...", zap.String("path", o.outputFile))
		if err := os.WriteFile(o.outputFile, []byte(strings.Join(res, "\n")), 0755); err != nil {
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
