package cmd

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/cmd/mirror"
	"github.com/gojekfarm/kat/cmd/topic"
	"github.com/spf13/cobra"
	"os"
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
	cliCmd.AddCommand(mirror.MirrorCmd)
}

func Execute() {
	if err := cliCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
