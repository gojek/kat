package cmd

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"os"
	"source.golabs.io/hermes/kat/cmd/topic"
)

var Admin sarama.ClusterAdmin

var cliCmd = &cobra.Command{
	Use:     "./kat",
	Short:   "Tool used for admin activities against specified kafka brokers",
	Version: fmt.Sprintf("%s (Commit: %s)", "0.0.1", "n/a"),
}

func init() {
	cobra.OnInitialize()
	cliCmd.AddCommand(topic.TopicCmd)
}

func Execute() {
	if err := cliCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
