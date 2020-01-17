package cmd

import (
	"github.com/gojekfarm/kat/cmd/admin"
	"github.com/gojekfarm/kat/cmd/config"
	"github.com/gojekfarm/kat/cmd/delete"
	"github.com/gojekfarm/kat/cmd/describe"
	"github.com/gojekfarm/kat/cmd/list"
	"github.com/gojekfarm/kat/logger"
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
