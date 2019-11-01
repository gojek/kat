package topicutil

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

func ListAll(admin sarama.ClusterAdmin) []string {
	var topicList []string
	topicDetails := ListTopicDetails(admin)
	if topicDetails == nil {
		return nil
	}
	for topic := range topicDetails {
		topicList = append(topicList, topic)
	}
	return topicList
}

func ListTopicDetails(admin sarama.ClusterAdmin) map[string]sarama.TopicDetail {
	topicDetails, err := admin.ListTopics()
	if err != nil {
		fmt.Printf("Err while retrieving Topic details: %v\n", err)
		return nil
	}
	return topicDetails
}

func DescribeConfig(admin sarama.ClusterAdmin, topic string) []sarama.ConfigEntry {
	configs, err := admin.DescribeConfig(sarama.ConfigResource{Name: topic, Type: sarama.TopicResource})
	if err != nil {
		fmt.Printf("Err while fetching config for Topic - %v: %v\n", topic, err)
		return nil
	}
	if len(configs) == 0 {
		fmt.Printf("Config not found for Topic - %v\n", topic)
		return nil
	}
	return configs
}

func ConfigString(configs []sarama.ConfigEntry, topic string) string {
	var topicConfig []string
	for _, config := range configs {
		topicConfig = append(topicConfig, config.Name+"="+config.Value)
	}
	cfg := strings.Join(topicConfig[:], ",")
	return cfg
}

func DescribeTopicMetadata(admin sarama.ClusterAdmin, topics []string) []*sarama.TopicMetadata {
	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Error while retrieving topicutil information %v\n", err)
		return nil
	}
	return metadata
}

func ConfigMap(configStr string) map[string]*string {
	configMap := make(map[string]*string)
	configs := strings.Split(configStr, ",")
	for _, config := range configs {
		configArr := strings.Split(config, "=")
		configMap[configArr[0]] = &configArr[1]
	}
	return configMap
}

func CreateTopic(admin sarama.ClusterAdmin, topic string, detail *sarama.TopicDetail, validateOnly bool, dryRun bool) []Output {
	if dryRun {
		op := Output{Topic: topic, Action: Create, ConfigChange: toConfigJSON(detail.ConfigEntries), Status: DryRun}
		return []Output{op}
	}
	err := admin.CreateTopic(topic, detail, validateOnly)
	if err != nil {
		op := Output{Topic: topic, Action: Create, ConfigChange: toConfigJSON(detail.ConfigEntries), Status: Failure}
		return []Output{op}
	}
	op := Output{Topic: topic, Action: Create, ConfigChange: toConfigJSON(detail.ConfigEntries), Status: Success}
	return []Output{op}
}
