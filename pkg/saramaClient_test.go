package pkg

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSaramaClient_ListTopicDetailsSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	saramaTopicDetail := map[string]sarama.TopicDetail{
		"topic1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
			ReplicaAssignment: nil,
			ConfigEntries:     nil,
		},
	}
	expectedTopicDetails := map[string]TopicDetail{
		"topic1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
			ReplicaAssignment: nil,
			Config:            nil,
		},
	}
	admin.On("ListTopics").Return(saramaTopicDetail, nil)

	topicDetails, err := client.ListTopicDetails()
	assert.NoError(t, err)
	assert.Equal(t, expectedTopicDetails, topicDetails)
	admin.AssertExpectations(t)
}

func TestSaramaClient_ListTopicDetailsFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	expectedErr := errors.New("error")
	admin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, expectedErr)

	_, err := client.ListTopicDetails()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_DescribeTopicMetadataSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	saramaTopicMetadata := []*sarama.TopicMetadata{
		{
			Err:        0,
			Name:       "topic1",
			IsInternal: false,
			Partitions: nil,
		},
	}
	expectedTopicMetadata := []*TopicMetadata{
		{
			Err:        sarama.ErrNoError,
			Name:       "topic1",
			IsInternal: false,
			Partitions: nil,
		},
	}
	topics := []string{"topic1"}
	admin.On("DescribeTopics", topics).Return(saramaTopicMetadata, nil)

	topicMetadata, err := client.DescribeTopicMetadata(topics)
	assert.NoError(t, err)
	assert.Equal(t, expectedTopicMetadata, topicMetadata)
	admin.AssertExpectations(t)
}

func TestSaramaClient_DescribeTopicMetadataFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	expectedErr := errors.New("error")
	topics := []string{"topic1"}
	admin.On("DescribeTopics", topics).Return([]*sarama.TopicMetadata{}, expectedErr)

	_, err := client.DescribeTopicMetadata(topics)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_UpdateConfigSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)

	topic := "topic1"
	entries := map[string]*string{}
	validateOnly := false
	admin.On("AlterConfig", sarama.TopicResource, topic, entries, validateOnly).Return(nil)

	err := client.UpdateConfig(client.GetTopicResourceType(), topic, entries, validateOnly)
	assert.NoError(t, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_UpdateConfigFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	expectedErr := errors.New("error")

	topic := "topic1"
	entries := map[string]*string{}
	validateOnly := false
	admin.On("AlterConfig", sarama.TopicResource, topic, entries, validateOnly).Return(expectedErr)

	err := client.UpdateConfig(client.GetTopicResourceType(), topic, entries, validateOnly)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_ShowConfigSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	configResource := ConfigResource{
		Type:        client.GetTopicResourceType(),
		Name:        "topic1",
		ConfigNames: nil,
	}
	saramaConfigResource := sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        "topic1",
		ConfigNames: nil,
	}
	saramaConfigEntries := []sarama.ConfigEntry{
		{
			Name:      "key1",
			Value:     "val1",
			ReadOnly:  false,
			Default:   false,
			Source:    0,
			Sensitive: false,
			Synonyms:  nil,
		},
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

	admin.On("DescribeConfig", saramaConfigResource).Return(saramaConfigEntries, nil)

	configEntries, err := client.ShowConfig(configResource)
	assert.NoError(t, err)
	assert.Equal(t, expectedConfigEntries, configEntries)
	admin.AssertExpectations(t)
}

func TestSaramaClient_ShowConfigFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := NewSaramaClient(admin, nil)
	expectedErr := errors.New("error")

	configResource := ConfigResource{
		Type:        client.GetTopicResourceType(),
		Name:        "topic1",
		ConfigNames: nil,
	}
	saramaConfigResource := sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        "topic1",
		ConfigNames: nil,
	}

	admin.On("DescribeConfig", saramaConfigResource).Return([]sarama.ConfigEntry{}, expectedErr)

	_, err := client.ShowConfig(configResource)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}
