package cmd

import (
	"fmt"
	"math"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"
)

type zipcopyOptions struct {
	*options

	source string
	target string

	maxBatchCount       int
	maxBatchSize        int
	archivingGoroutines int
	batchAcrossPages    bool
}

func NewZipCopyCommand(opts *options) *cobra.Command {
	var zipcopyOpts zipcopyOptions
	zipcopyOpts.options = opts
	command := &cobra.Command{
		Use:     "zipcopy",
		Aliases: []string{"zcopy", "zcp", "zp"},
		Short:   "copy and zip batches",
		Example: "fassst zcopy path/to/source path/to/target",
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 2 {
				zipcopyOpts.target = pkgfs.MakeSureHasSuffix(args[1], "/")
			} else {
				zipcopyOpts.target = "./"
			}
			zipcopyOpts.source = pkgfs.MakeSureHasSuffix(args[0], "/")
			return zipcopyOpts.run()
		},
	}

	command.Flags().IntVarP(&zipcopyOpts.maxBatchCount,
		"max-batch-count",
		"c",
		0,
		"maximum amount of files per archive (0 for unbounded)",
	)
	command.Flags().IntVarP(&zipcopyOpts.maxBatchSize,
		"max-batch-size",
		"s",
		0,
		"maximum amount of files per archive(in MB) (0 for unbounded)",
	)
	command.Flags().IntVarP(&zipcopyOpts.archivingGoroutines,
		"archiving-goroutines",
		"r",
		0,
		"number of concurrent archiving (0 to use listing-goroutines value)",
	)
	command.Flags().BoolVarP(&zipcopyOpts.batchAcrossPages,
		"batch-across-pages",
		"b",
		false,
		"allow more than 1 page per batch",
	)
	return command
}

func (o zipcopyOptions) run() error {
	url, fs, err := pkgfs.FileSystemByUrl(o.source)
	if err != nil {
		return fmt.Errorf("source file system from url: %w", err)
	}
	urlTarget, fsTarget, err := pkgfs.FileSystemByUrl(o.target)
	if err != nil {
		return fmt.Errorf("target file system from url: %w", err)
	}
	if o.maxBatchCount == 0 {
		o.log.Info("max-batch-count: unbounded")
		o.maxBatchCount = math.MaxInt
	}
	if o.maxBatchSize == 0 {
		o.log.Info("max-batch-size: unbounded")
		o.maxBatchSize = math.MaxInt
	}
	o.log.Info("zip copying...")
	if o.batchAcrossPages {
		if o.archivingGoroutines == 0 {
			o.log.Info("archiving-goroutines defaulting to listing-goroutines value", zap.Int("value", o.listingGoroutines))
			o.archivingGoroutines = o.listingGoroutines
		}
		fst.ZipCopyAcrossPages(fs, fsTarget, url, urlTarget, o.listingGoroutines, o.archivingGoroutines, o.maxBatchCount, o.maxBatchSize, o.log)
	} else {
		fst.ZipCopyPages(fs, fsTarget, url, urlTarget, o.listingGoroutines, o.maxBatchCount, o.maxBatchSize, o.log)
	}
	return nil
}
