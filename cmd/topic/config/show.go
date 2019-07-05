package config

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kafka-admin-tools/util"
)

var showCmd = &cobra.Command{
	Use:   "show",
	Short: "shows the config for the given topics",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		admin := u.GetAdminClient()
		topics := u.GetTopicNames()
		show(admin, topics)
	},
}

func show(admin sarama.ClusterAdmin, topics []string) {
	for _, topic := range topics {
		configs, err := admin.DescribeConfig(sarama.ConfigResource{Name: topic, Type: sarama.TopicResource})
		if err != nil {
			fmt.Printf("Err while fetching config for topic - %v: %v\n", topic, err)
			continue
		}
		if len(configs) == 0 {
			fmt.Printf("Config not found for topic - %v\n", topic)
			continue
		}
		fmt.Println("---------------------------------------------")
		fmt.Printf("Configuration for topic - %v\n", topic)
		fmt.Println("---------------------------------------------")
		for _, config := range configs {
			fmt.Println(config)
		}
	}
}
