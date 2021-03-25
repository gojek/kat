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

func (t *Topic) ListTopicWithSizeLessThanOrEqualTo(size int64) ([]string, error) {
	brokerMap := t.apiClient.ListBrokers()
	brokerIDs := make([]int32, 0, len(brokerMap))
	for brokerID := range brokerMap {
		brokerIDs = append(brokerIDs, int32(brokerID))
	}
	metaData, err := t.apiClient.DescribeLogDirs(brokerIDs)
	if err != nil {
		return nil, err
	}
	topicWiseMap, err := getTopicWiseMetaDataMap(metaData)
	if err != nil {
		return nil, err
	}
	emptyTopics := filterByTopicSize(topicWiseMap, size)
	return emptyTopics, nil
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

func filterByTopicSize(topicWiseMap map[string][]client.DescribeLogDirsResponsePartition, size int64) []string {
	sizeFilteredTopics := make([]string, 0)
	for topic, partitionMetaDataSlice := range topicWiseMap {
		total := int64(0)
		for _, partitionMetaData := range partitionMetaDataSlice {
			total += partitionMetaData.Size
		}
		if size >= total {
			sizeFilteredTopics = append(sizeFilteredTopics, topic)
		}
	}
	return sizeFilteredTopics
}

func getTopicWiseMetaDataMap(brokerMetaDataMap map[int32][]client.
	DescribeLogDirsResponseDirMetadata) (map[string][]client.DescribeLogDirsResponsePartition, error) {

	topicWiseMap := make(map[string][]client.DescribeLogDirsResponsePartition)
	for _, brokerWiseMetaData := range brokerMetaDataMap {
		for _, logDirsMeta := range brokerWiseMetaData {
			if logDirsMeta.Error == nil {
				for _, topicWiseMetaData := range logDirsMeta.Topics {
					topic := topicWiseMetaData.Topic
					if topicWiseMap[topic] == nil {
						topicWiseMap[topic] = make([]client.DescribeLogDirsResponsePartition, 0)
					}
					topicWiseMap[topic] = append(topicWiseMap[topic], topicWiseMetaData.Partitions...)
				}
			} else {
				return nil, logDirsMeta.Error
			}
		}
	}
	return topicWiseMap, nil
}
