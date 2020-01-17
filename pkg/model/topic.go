package model

import (
	"github.com/gojek/kat/logger"
	"github.com/gojek/kat/pkg/client"
)

type Topic struct {
	apiClient client.KafkaAPIClient
	sshClient client.KafkaSSHClient
}

func NewTopic(apiClient client.KafkaAPIClient, opts ...TopicOpts) (*Topic, error) {
	topic := &Topic{apiClient: apiClient}

	for _, opt := range opts {
		err := opt(topic)
		if err != nil {
			return nil, err
		}
	}

	return topic, nil
}

type TopicOpts func(*Topic) error

func WithSSHClient(user, port, keyfile string) TopicOpts {
	return func(t *Topic) error {
		sshClient, err := client.NewSSHClient(user, port, keyfile)
		if err != nil {
			return err
		}
		kafkaSSHClient, err := client.NewKafkaRemoteClient(t.apiClient, sshClient)
		if err != nil {
			logger.Errorf("Error while creating kafka remote client - %v\n", err)
			return err
		}
		t.sshClient = kafkaSSHClient
		return nil
	}
}

func (t *Topic) Create(topic string, detail client.TopicDetail, validateOnly bool) error {
	return t.apiClient.CreateTopic(topic, detail, validateOnly)
}

func (t *Topic) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return t.apiClient.CreatePartitions(topic, count, assignment, validateOnly)
}

func (t *Topic) List() (map[string]client.TopicDetail, error) {
	return t.apiClient.ListTopicDetails()
}

func (t *Topic) ListLastWrittenTopics(lastWrittenEpoch int64, dataDir string) ([]string, error) {
	return t.sshClient.ListTopics(client.ListTopicsRequest{
		LastWritten: lastWrittenEpoch,
		DataDir:     dataDir,
	})
}

func (t *Topic) ListOnly(regex string, include bool) ([]string, error) {
	topicDetails, err := t.List()
	if err != nil {
		return nil, err
	}

	var topics []string
	for key := range topicDetails {
		topics = append(topics, key)
	}
	return ListUtil{topics}.Filter(regex, include)
}

func (t *Topic) Describe(topics []string) ([]*client.TopicMetadata, error) {
	return t.apiClient.DescribeTopicMetadata(topics)
}

func (t *Topic) GetConfig(topic string) ([]client.ConfigEntry, error) {
	configResource := client.ConfigResource{Name: topic, Type: t.apiClient.GetTopicResourceType()}
	return t.apiClient.GetConfig(configResource)
}

func (t *Topic) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	for _, topicName := range topics {
		err := t.apiClient.UpdateConfig(t.apiClient.GetTopicResourceType(), topicName, configMap, validateOnly)
		if err != nil {
			logger.Errorf("Err while updating config for topic - %v: %v\n", topicName, err)
			return err
		}
		logger.Infof("Configuration was successfully updated for topic - %v\n", topicName)
	}
	return nil
}

func (t *Topic) Delete(topics []string) error {
	return t.apiClient.DeleteTopic(topics)
}
