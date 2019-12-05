package pkg

import (
	"fmt"
	"regexp"
)

type Topic struct {
	client KafkaClient
}

type TopicCli interface {
	List() (map[string]TopicDetail, error)
	Get(regex string) ([]string, error)
	Describe(topics []string) ([]*TopicMetadata, error)
	ShowConfig(topic string) ([]ConfigEntry, error)
	UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error
	IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, zookeeper string) error
	ReassignPartitions(topics []string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, brokerList, zookeeper string) error
}

func NewTopic(client KafkaClient) *Topic {
	return &Topic{client: client}
}

func (t *Topic) List() (map[string]TopicDetail, error) {
	return t.client.ListTopicDetails()
}

func (t *Topic) Get(regex string) ([]string, error) {
	topicDetails, err := t.List()
	if err != nil {
		return nil, err
	}

	var topics []string
	for key := range topicDetails {
		matched, err := regexp.Match(regex, []byte(key))
		if err != nil {
			return nil, err
		}

		if matched {
			topics = append(topics, key)
		}
	}
	return topics, nil
}

func (t *Topic) Describe(topics []string) ([]*TopicMetadata, error) {
	return t.client.DescribeTopicMetadata(topics)
}

func (t *Topic) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	for _, topicName := range topics {
		err := t.client.UpdateConfig(t.client.GetTopicResourceType(), topicName, configMap, validateOnly)
		if err != nil {
			fmt.Printf("Err while updating config for topic - %v: %v\n", topicName, err)
			return err
		}
		fmt.Printf("Config was successfully updated for topic - %v\n", topicName)
	}
	return nil
}

func (t *Topic) ShowConfig(topic string) ([]ConfigEntry, error) {
	configResource := ConfigResource{Name: topic, Type: t.client.GetTopicResourceType()}
	return t.client.ShowConfig(configResource)
}

func (t *Topic) ReassignPartitions(topics []string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, brokerList, zookeeper string) error {
	return NewPartition(zookeeper).ReassignPartitions(topics, brokerList, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
}

func (t *Topic) IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, zookeeper string) error {
	metadata, err := t.Describe(topics)
	if err != nil {
		fmt.Printf("Error while fetching topic metadata: %v\n", err)
		return err
	}
	return NewPartition(zookeeper).IncreaseReplication(metadata, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
}
