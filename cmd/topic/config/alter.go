package config

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
	"strings"
)

type alter struct {
	admin  sarama.ClusterAdmin
	config string
	topics []string
}

var alterCmd = &cobra.Command{
	Use:   "alter",
	Short: "alter the config for the given topics",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		a := alter{admin: u.GetAdminClient("broker-list"), config: u.GetCmdArg("config"), topics: u.GetTopicNames()}
		a.alter()
	},
}

func init() {
	alterCmd.PersistentFlags().StringP("config", "c", "", "Comma separated list of configs, eg: key1=val1,key2=val2")
	alterCmd.MarkPersistentFlagRequired("config")
}

func (a *alter) alter() {
	configMap := configMap(a.config)

	for _, topic := range a.topics {
		err := a.admin.AlterConfig(sarama.TopicResource, topic, configMap, false)
		if err != nil {
			fmt.Printf("Err while altering config for topic - %v: %v\n", topic, err)
			continue
		} else {
			fmt.Printf("Config was successfully altered for topic - %v\n", topic)
		}
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
