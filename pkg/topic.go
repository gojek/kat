package pkg

import (
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/util"
)

type Topic struct {
	apiClient KafkaAPIClient
	sshClient KafkaSSHClient
}

type TopicCli interface {
	Create(topic string, detail TopicDetail, validateOnly bool) error
	List() (map[string]TopicDetail, error)
	ListLastWrittenTopics(int64, string) ([]string, error)
	ListOnly(regex string, include bool) ([]string, error)
	Delete(topics []string) error
	Describe(topics []string) ([]*TopicMetadata, error)
	GetConfig(topic string) ([]ConfigEntry, error)
	UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error
	IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, zookeeper string) error
	ReassignPartitions(topics []string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, brokerList, zookeeper string) error
	CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error
}

func NewTopic(apiClient KafkaAPIClient, opts ...TopicOpts) (*Topic, error) {
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
		sshClient, err := NewSSHClient(user, port, keyfile)
		if err != nil {
			return err
		}
		kafkaSSHClient, err := NewKafkaRemoteClient(t.apiClient, sshClient)
		if err != nil {
			logger.Errorf("Error while creating kafka remote client - %v\n", err)
			return err
		}
		t.sshClient = kafkaSSHClient
		return nil
	}
}

func (t *Topic) Create(topic string, detail TopicDetail, validateOnly bool) error {
	return t.apiClient.CreateTopic(topic, detail, validateOnly)
}

func (t *Topic) List() (map[string]TopicDetail, error) {
	return t.apiClient.ListTopicDetails()
}

func (t *Topic) ListLastWrittenTopics(lastWrittenEpoch int64, dataDir string) ([]string, error) {
	return t.sshClient.ListTopics(ListTopicsRequest{
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
	return util.Filter(topics, regex, include)
}

func (t *Topic) Delete(topics []string) error {
	return t.apiClient.DeleteTopic(topics)
}

func (t *Topic) Describe(topics []string) ([]*TopicMetadata, error) {
	return t.apiClient.DescribeTopicMetadata(topics)
}

func (t *Topic) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	for _, topicName := range topics {
		err := t.apiClient.UpdateConfig(t.apiClient.GetTopicResourceType(), topicName, configMap, validateOnly)
		if err != nil {
			logger.Errorf("Err while updating config for topic - %v: %v\n", topicName, err)
			return err
		}
		logger.Infof("Config was successfully updated for topic - %v\n", topicName)
	}
	return nil
}

func (t *Topic) GetConfig(topic string) ([]ConfigEntry, error) {
	configResource := ConfigResource{Name: topic, Type: t.apiClient.GetTopicResourceType()}
	return t.apiClient.GetConfig(configResource)
}

func (t *Topic) ReassignPartitions(topics []string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, brokerList, zookeeper string) error {
	return NewPartition(zookeeper).ReassignPartitions(topics, brokerList, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
}

func (t *Topic) IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle int, zookeeper string) error {
	metadata, err := t.Describe(topics)
	if err != nil {
		logger.Errorf("Error while fetching topic metadata: %v\n", err)
		return err
	}
	return NewPartition(zookeeper).IncreaseReplication(metadata, replicationFactor, numOfBrokers, batch, timeoutPerBatchInS, pollIntervalInS, throttle)
}

func (t *Topic) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return t.apiClient.CreatePartitions(topic, count, assignment, validateOnly)
}
