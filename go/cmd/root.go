package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type options struct {
	listingGoroutines int

	log     *zap.Logger
	verbose bool
}

func (o options) Validate() error {
	if o.listingGoroutines < 1 {
		return fmt.Errorf("listing-goroutines must be at least 1, was %d", o.listingGoroutines)
	}
	return nil
}

// NewCommand creates a new root command.
func NewCommand() *cobra.Command {
	var opts options
	var startTime time.Time
	command := &cobra.Command{
		Use:          "fassst",
		Short:        "go fassst!",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			var err error
			// TODO: configure log properly?
			if opts.verbose {
				if opts.log, err = zap.NewDevelopment(); err != nil {
					panic(fmt.Errorf("new zap dev logger: %w", err))
				}
			} else {
				if opts.log, err = zap.NewProduction(); err != nil {
					panic(fmt.Errorf("new zap prod logger: %w", err))
				}
			}
			err = opts.Validate()
			if err != nil {
				return fmt.Errorf("validate opts: %w", err)
			}
			startTime = time.Now()
			return nil
		},
		PersistentPostRun: func(_ *cobra.Command, _ []string) {
			delta := time.Since(startTime)
			opts.log.Info("fassst!", zap.Duration("run time", delta))
			opts.log.Sync()
		},
	}

	command.AddCommand(
		NewVersionCommand(),
		NewListCommand(&opts),
		NewCopyCommand(&opts),
		NewSyncCommand(&opts),
		NewZipCopyCommand(&opts),
	)

	command.PersistentFlags().IntVarP(&opts.listingGoroutines,
		"listing-goroutines",
		"l",
		30,
		"number of concurrent listing goroutines",
	)

	command.PersistentFlags().BoolVarP(&opts.verbose,
		"verbose",
		"v",
		false,
		"print debug info",
	)

	return command
}
