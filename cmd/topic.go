package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"github.com/spf13/cobra"
)

var TopicCli pkg.TopicCli

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Admin commands on topics",
}

func loadTopicCli(command *cobra.Command, args []string) {
	LoadCobra(command, args)
	TopicCli = pkg.NewTopic(pkg.NewSaramaClient(Cobra.GetSaramaClient("broker-list")))
}

func clearTopicCli(command *cobra.Command, args []string) {
	TopicCli = nil
	ClearCobra(command, args)
}

func init() {
	topicCmd.PersistentFlags().StringP("broker-list", "b", "", "Comma separated list of broker ips")
	topicCmd.MarkPersistentFlagRequired("broker-list")

	topicCmd.AddCommand(listTopicCmd)
	topicCmd.AddCommand(describeTopicCmd)
	topicCmd.AddCommand(increaseReplicationFactorCmd)
	topicCmd.AddCommand(reassignPartitionsCmd)
	topicCmd.AddCommand(configCmd)

}
