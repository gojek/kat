package config

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kafka-admin-tools/util"
	"strings"
)

var alterCmd = &cobra.Command{
	Use:   "alter",
	Short: "alter the config for the given topics",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		admin := u.GetAdminClient()
		topics := u.GetTopicNames()
		config := u.GetCmdArg("config")
		alter(admin, topics, config)
	},
}

func init() {
	alterCmd.PersistentFlags().StringP("config", "c", "", "Comma separated list of configs, eg: key1=val1,key2=val2")
	alterCmd.MarkPersistentFlagRequired("config")
}

func alter(admin sarama.ClusterAdmin, topics []string, config string) {
	configMap := configMap(config)

	for _, topic := range topics {
		err := admin.AlterConfig(sarama.TopicResource, topic, configMap, false)
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
