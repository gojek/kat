package model

import (
	"errors"
	"testing"

	"github.com/gojek/kat/pkg/client"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestTopic_ListSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedTopicDetails := map[string]client.TopicDetail{
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
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	kafkaClient.On("ListTopicDetails").Return(map[string]client.TopicDetail{}, expectedErr)

	_, err = topicCli.List()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_DescribeSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedTopicMetadata := []*client.TopicMetadata{
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
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topics := []string{"topic1"}
	kafkaClient.On("DescribeTopicMetadata", topics).Return([]*client.TopicMetadata{}, expectedErr)

	_, err = topicCli.Describe(topics)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_UpdateConfigSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)

	topics := []string{"topic1"}
	entries := map[string]*string{}
	validateOnly := false
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	kafkaClient.On("UpdateConfig", int(sarama.TopicResource), topics[0], entries, validateOnly).Return(nil)

	err = topicCli.UpdateConfig(topics, entries, validateOnly)
	assert.NoError(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_UpdateConfigFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topics := []string{"topic1"}
	entries := map[string]*string{}
	validateOnly := false
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	kafkaClient.On("UpdateConfig", int(sarama.TopicResource), topics[0], entries, validateOnly).Return(expectedErr)

	err = topicCli.UpdateConfig(topics, entries, validateOnly)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ShowConfigSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)

	topic := "topic1"
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	configResource := client.ConfigResource{
		Type:        kafkaClient.GetTopicResourceType(),
		Name:        topic,
		ConfigNames: nil,
	}
	expectedConfigEntries := []client.ConfigEntry{
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

	kafkaClient.On("GetConfig", configResource).Return(expectedConfigEntries, nil)

	configEntries, err := topicCli.GetConfig(topic)
	assert.NoError(t, err)
	assert.Equal(t, expectedConfigEntries, configEntries)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ShowConfigFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, err := NewTopic(kafkaClient)
	expectedErr := errors.New("error")

	topic := "topic1"
	kafkaClient.On("GetTopicResourceType").Return(int(sarama.TopicResource))
	configResource := client.ConfigResource{
		Type:        kafkaClient.GetTopicResourceType(),
		Name:        topic,
		ConfigNames: nil,
	}

	kafkaClient.On("GetConfig", configResource).Return([]client.ConfigEntry{}, expectedErr)

	_, err = topicCli.GetConfig(topic)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_DeleteSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topics := []string{"topic-1", "topic-2"}
	kafkaClient.On("DeleteTopic", topics).Return(nil)

	err := topicCli.Delete(topics)

	assert.NoError(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_DeleteFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topics := []string{"topic-1", "topic-2"}
	kafkaClient.On("DeleteTopic", topics).Return(errors.New("error"))

	err := topicCli.Delete(topics)

	assert.Error(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_CreateSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topicName := "topic-1"
	detail := client.TopicDetail{}
	validateOnly := false
	kafkaClient.On("CreateTopic", topicName, detail, validateOnly).Return(nil)

	err := topicCli.Create(topicName, detail, validateOnly)

	assert.NoError(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_CreateFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topicName := "topic-1"
	detail := client.TopicDetail{}
	validateOnly := false
	kafkaClient.On("CreateTopic", topicName, detail, validateOnly).Return(errors.New("error"))

	err := topicCli.Create(topicName, detail, validateOnly)

	assert.Error(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_CreatePartitionsSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topicName := "topic-1"
	count := int32(10)
	assignment := [][]int32{}
	validateOnly := false
	kafkaClient.On("CreatePartitions", topicName, count, assignment, validateOnly).Return(nil)

	err := topicCli.CreatePartitions(topicName, count, assignment, validateOnly)

	assert.NoError(t, err)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_CreatePartitionsFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	topicName := "topic-1"
	count := int32(10)
	assignment := [][]int32{}
	validateOnly := false
	kafkaClient.On("CreatePartitions", topicName, count, assignment, validateOnly).Return(errors.New("error"))

	err := topicCli.CreatePartitions(topicName, count, assignment, validateOnly)

	assert.Error(t, err)
	kafkaClient.AssertExpectations(t)
}
