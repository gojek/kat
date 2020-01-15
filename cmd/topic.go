package cmd

import (
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/spf13/cobra"
)

var TopicCli pkg.TopicCli

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Admin commands on topics",
}

func init() {
	topicCmd.PersistentFlags().StringP("broker-list", "b", "", "Comma separated list of broker ips")
	if err := topicCmd.MarkPersistentFlagRequired("broker-list"); err != nil {
		logger.Fatal(err)
	}

	topicCmd.AddCommand(listTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(describeTopicCmd)
	topicCmd.AddCommand(increaseReplicationFactorCmd)
	topicCmd.AddCommand(reassignPartitionsCmd)
	topicCmd.AddCommand(configCmd)

}
