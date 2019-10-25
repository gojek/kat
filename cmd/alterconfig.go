package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type alterConfig struct {
	config string
	topics []string
}

var alterConfigCmd = &cobra.Command{
	Use:   "alter",
	Short: "alter the config for the given topics",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		a := alterConfig{config: Cobra.GetCmdArg("config"), topics: Cobra.GetTopicNames()}
		a.alterConfig()
	},
	PostRun: clearTopicCli,
}

func init() {
	alterConfigCmd.PersistentFlags().StringP("config", "c", "", "Comma separated list of configs, eg: key1=val1,key2=val2")
	alterConfigCmd.MarkPersistentFlagRequired("config")
}

func (a *alterConfig) alterConfig() {
	configMap := configMap(a.config)
	err := TopicCli.UpdateConfig(a.topics, configMap, false)
	if err != nil {
		fmt.Printf("Error while altering config - %v\n", err)
	}
}

func configMap(configStr string) map[string]*string {
	configMap := make(map[string]*string)
	configs := strings.Split(configStr, ",")
	for _, config := range configs {
		configArr := strings.Split(config, "=")
		configMap[configArr[0]] = &configArr[1]
	}
	return configMap
}
