package topicutil

import (
	"fmt"
	"strings"
)
import "github.com/Shopify/sarama"

func ListAll(admin sarama.ClusterAdmin) []string {
	var topicList []string
	topicDetails := ListTopicDetails(admin)
	if topicDetails == nil {
		return nil
	}
	for topicDetail := range topicDetails {
		topicList = append(topicList, topicDetail)
	}
	return topicList
}

func ListTopicDetails(admin sarama.ClusterAdmin) map[string]sarama.TopicDetail {
	topicDetails, err := admin.ListTopics()
	if err != nil {
		fmt.Printf("Err while retrieving topic details: %v\n", err)
		return nil
	}
	return topicDetails
}

func DescribeConfig(admin sarama.ClusterAdmin, topic string) []sarama.ConfigEntry {
	configs, err := admin.DescribeConfig(sarama.ConfigResource{Name: topic, Type: sarama.TopicResource})
	if err != nil {
		fmt.Printf("Err while fetching config for topic - %v: %v\n", topic, err)
		return nil
	}
	if len(configs) == 0 {
		fmt.Printf("Config not found for topic - %v\n", topic)
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