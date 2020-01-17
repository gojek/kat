package config

import (
	"strings"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/cmd/base"

	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

type alterConfig struct {
	client.Configurer
	config string
	topics []string
}

var alterConfigCmd = &cobra.Command{
	Use:   "alter",
	Short: "alter the config for the given topics",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		a := alterConfig{Configurer: base.Init(cobraUtil).GetTopic(), config: cobraUtil.GetStringArg("config"), topics: cobraUtil.GetTopicNames()}
		a.alterConfig()
	},
}

func init() {
	alterConfigCmd.PersistentFlags().StringP("config", "c", "", "Comma separated list of configs, eg: key1=val1,key2=val2")
	if err := alterConfigCmd.MarkPersistentFlagRequired("config"); err != nil {
		logger.Fatal(err)
	}
}

func (a *alterConfig) alterConfig() {
	configMap := configMap(a.config)
	err := a.UpdateConfig(a.topics, configMap, false)
	if err != nil {
		logger.Fatalf("Error while altering config - %v\n", err)
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
