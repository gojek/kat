package cmd

import (
	"github.com/gojek/kat/cmd/list"
	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

var consumerGroupCmd = &cobra.Command{
	Use:   "consumergroup",
	Short: "Admin commands on consumergroups",
}

func init() {
	consumerGroupCmd.PersistentFlags().StringP("broker-list", "b", "", "Comma separated list of broker ips")
	consumerGroupCmd.PersistentFlags().StringP("topic", "t", "", "Specify topic")
	if err := consumerGroupCmd.MarkPersistentFlagRequired("broker-list"); err != nil {
		logger.Fatal(err)
	}

	consumerGroupCmd.AddCommand(list.ListConsumerGroupsCmd)
}
