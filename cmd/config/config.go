package config

import (
	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

var ConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Config of topics",
}

func init() {
	ConfigCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names for which retention needs to be changed")
	if err := ConfigCmd.MarkPersistentFlagRequired("topics"); err != nil {
		logger.Fatal(err)
	}
	ConfigCmd.AddCommand(showConfigCmd)
	ConfigCmd.AddCommand(alterConfigCmd)
}
