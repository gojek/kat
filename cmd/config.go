package cmd

import (
	"github.com/gojekfarm/kat/logger"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Config of topics",
}

func init() {
	configCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names for which retention needs to be changed")
	if err := configCmd.MarkPersistentFlagRequired("topics"); err != nil {
		logger.Fatal(err)
	}
	configCmd.AddCommand(showConfigCmd)
	configCmd.AddCommand(alterConfigCmd)
}
