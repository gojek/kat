package config

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/topicutil"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

type show struct {
	admin  sarama.ClusterAdmin
	topics []string
}

var showCmd = &cobra.Command{
	Use:   "show",
	Short: "shows the config for the given topics",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		s := show{admin: u.GetAdminClient("broker-list"), topics: u.GetTopicNames()}
		s.show()
	},
}

func (s *show) show() {
	for _, topic := range s.topics {
		configs := topicutil.DescribeConfig(s.admin, topic)
		if configs == nil {
			continue
		}
		fmt.Println("---------------------------------------------")
		fmt.Printf("Configuration for topic - %v\n", topic)
		fmt.Println("---------------------------------------------")
		for _, config := range configs {
			fmt.Printf("%+v\n", config)
		}
	}
}
