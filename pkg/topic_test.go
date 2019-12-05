package pkg

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTopic_ListSuccess(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedTopicDetails := map[string]TopicDetail{
		"topic1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
			ReplicaAssignment: nil,
			Config:            nil,
		},
	}
	kafkaClient.On("ListTopicDetails").Return(expectedTopicDetails, nil)

	topicDetails, err := topicCli.List()
	assert.NoError(t, err)
	assert.Equal(t, expectedTopicDetails, topicDetails)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ListFailure(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	kafkaClient.On("ListTopicDetails").Return(map[string]TopicDetail{}, expectedErr)

	_, err := topicCli.List()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_DescribeSuccess(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedTopicMetadata := []*TopicMetadata{
		{
			Err:        sarama.ErrNoError,
			Name:       "topic1",
			IsInternal: false,
			Partitions: nil,
		},
	}
	topics := []string{"topic1"}
	kafkaClient.On("DescribeTopicMetadata", topics).Return(expectedTopicMetadata, nil)

	topicMetadata, err := topicCli.Describe(topics)
	assert.NoError(t, err)
	assert.Equal(t, expectedTopicMetadata, topicMetadata)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_DescribeFailure(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topics := []string{"topic1"}
	kafkaClient.On("DescribeTopicMetadata", topics).Return([]*TopicMetadata{}, expectedErr)

	_, err := topicCli.Describe(topics)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_UpdateConfigSuccess(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)

	topics := []string{"topic1"}
	entries := map[string]*string{}
	validateOnly := false
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	kafkaClient.On("UpdateConfig", int(sarama.TopicResource), topics[0], entries, validateOnly).Return(nil)

	err := topicCli.UpdateConfig(topics, entries, validateOnly)
	assert.NoError(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_UpdateConfigFailure(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topics := []string{"topic1"}
	entries := map[string]*string{}
	validateOnly := false
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	kafkaClient.On("UpdateConfig", int(sarama.TopicResource), topics[0], entries, validateOnly).Return(expectedErr)

	err := topicCli.UpdateConfig(topics, entries, validateOnly)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ShowConfigSuccess(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)

	topic := "topic1"
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	configResource := ConfigResource{
		Type:        kafkaClient.GetTopicResourceType(),
		Name:        topic,
		ConfigNames: nil,
	}
	expectedConfigEntries := []ConfigEntry{
		{
			Name:      "key1",
			Value:     "val1",
			ReadOnly:  false,
			Default:   false,
			Source:    "Unknown",
			Sensitive: false,
			Synonyms:  nil,
		},
	}

	kafkaClient.On("ShowConfig", configResource).Return(expectedConfigEntries, nil)

	configEntries, err := topicCli.ShowConfig(topic)
	assert.NoError(t, err)
	assert.Equal(t, expectedConfigEntries, configEntries)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ShowConfigFailure(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topic := "topic1"
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	configResource := ConfigResource{
		Type:        kafkaClient.GetTopicResourceType(),
		Name:        topic,
		ConfigNames: nil,
	}

	kafkaClient.On("ShowConfig", configResource).Return([]ConfigEntry{}, expectedErr)

	_, err := topicCli.ShowConfig(topic)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_IncreaseReplicationFactorDescribeFailure(t *testing.T) {
	kafkaClient := &MockKafkaClient{}
	topicCli := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topics := []string{"topic1"}
	kafkaClient.On("DescribeTopicMetadata", topics).Return([]*TopicMetadata{}, expectedErr)

	err := topicCli.IncreaseReplicationFactor(topics, 1, 1, 1, 1, 1, 10000, "zookeeper")
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}
