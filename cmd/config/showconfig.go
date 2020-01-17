package config

import (
	"fmt"

	"github.com/gojekfarm/kat/pkg/client"

	"github.com/gojekfarm/kat/cmd/base"

	"github.com/gojekfarm/kat/logger"
	"github.com/spf13/cobra"
)

type showConfig struct {
	client.Configurer
	topics []string
}

var showConfigCmd = &cobra.Command{
	Use:   "show",
	Short: "shows the config for the given topics",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		s := showConfig{Configurer: base.Init(cobraUtil).GetTopic(), topics: cobraUtil.GetTopicNames()}
		s.showConfig()
	},
}

func (s *showConfig) showConfig() {
	for _, topicName := range s.topics {
		configs, err := s.GetConfig(topicName)
		if err != nil {
			logger.Fatalf("Error while fetching config for topic %v - %v\n", topicName, err)
			return
		}
		if len(configs) == 0 {
			logger.Infof("Configs not found for topic - %v\n", topicName)
			continue
		}
		fmt.Println("---------------------------------------------")
		fmt.Printf("Configuration for topic - %v\n", topicName)
		fmt.Println("---------------------------------------------")
		for _, config := range configs {
			fmt.Printf("%+v\n", config)
		}
	}
}
