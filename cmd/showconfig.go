package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

type showConfig struct {
	topics []string
}

var showConfigCmd = &cobra.Command{
	Use:    "show",
	Short:  "shows the config for the given topics",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		s := showConfig{topics: Cobra.GetTopicNames()}
		s.showConfig()
	},
	PostRun: clearTopicCli,
}

func (s *showConfig) showConfig() {
	for _, topicName := range s.topics {
		configs, err := TopicCli.ShowConfig(topicName)
		if err != nil {
			fmt.Printf("Error while fetching config for topic %v - %v\n", topicName, err)
			return
		}
		fmt.Println("---------------------------------------------")
		fmt.Printf("Configuration for topic - %v\n", topicName)
		fmt.Println("---------------------------------------------")
		for _, config := range configs {
			fmt.Printf("%+v\n", config)
		}
	}
}
