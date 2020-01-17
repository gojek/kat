package cmd

import (
	"github.com/gojek/kat/cmd/admin"
	"github.com/gojek/kat/cmd/config"
	"github.com/gojek/kat/cmd/delete"
	"github.com/gojek/kat/cmd/describe"
	"github.com/gojek/kat/cmd/list"
	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Admin commands on topics",
}

func init() {
	topicCmd.PersistentFlags().StringP("broker-list", "b", "", "Comma separated list of broker ips")
	if err := topicCmd.MarkPersistentFlagRequired("broker-list"); err != nil {
		logger.Fatal(err)
	}

	topicCmd.AddCommand(list.ListTopicCmd)
	topicCmd.AddCommand(delete.DeleteTopicCmd)
	topicCmd.AddCommand(describe.DescribeTopicCmd)
	topicCmd.AddCommand(admin.IncreaseReplicationFactorCmd)
	topicCmd.AddCommand(admin.ReassignPartitionsCmd)
	topicCmd.AddCommand(config.ConfigCmd)

}
