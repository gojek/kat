package cmd

import (
	"strings"

	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/util"

	"github.com/spf13/cobra"
)

type alterConfig struct {
	BaseCmd
	config string
	topics []string
}

var alterConfigCmd = &cobra.Command{
	Use:   "alter",
	Short: "alter the config for the given topics",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := util.NewCobraUtil(command)
		baseCmd := Init(cobraUtil)
		a := alterConfig{BaseCmd: baseCmd, config: cobraUtil.GetStringArg("config"), topics: cobraUtil.GetTopicNames()}
		a.alterConfig()
	},
}

func init() {
	alterConfigCmd.PersistentFlags().StringP("config", "c", "", "Comma separated list of configs, eg: key1=val1,key2=val2")
	alterConfigCmd.MarkPersistentFlagRequired("config")
}

func (a *alterConfig) alterConfig() {
	configMap := configMap(a.config)
	err := a.TopicCli.UpdateConfig(a.topics, configMap, false)
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
