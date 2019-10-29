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

	err := topicCli.IncreaseReplicationFactor(topics, 1, 1, "path", "zookeeper")
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	kafkaClient.AssertExpectations(t)
}

func TestBuildReassignmentJson(suite *testing.T) {
	suite.Run("Build Reassignment Json", func(t *testing.T) {
		partitionMetadata1 := PartitionMetadata{ID: 8, Leader: 6, Replicas: []int32{6}}
		partitionMetadata2 := PartitionMetadata{ID: 11, Leader: 3, Replicas: []int32{3}}
		partitionMetadata3 := PartitionMetadata{ID: 2, Leader: 6, Replicas: []int32{6}}
		partitionMetadata4 := PartitionMetadata{ID: 5, Leader: 3, Replicas: []int32{3}}
		partitionMetadata5 := PartitionMetadata{ID: 4, Leader: 2, Replicas: []int32{2}}
		partitionMetadata6 := PartitionMetadata{ID: 7, Leader: 5, Replicas: []int32{5}}
		partitionMetadata7 := PartitionMetadata{ID: 10, Leader: 2, Replicas: []int32{2}}
		partitionMetadata8 := PartitionMetadata{ID: 1, Leader: 5, Replicas: []int32{5}}
		partitionMetadata9 := PartitionMetadata{ID: 9, Leader: 1, Replicas: []int32{1}}
		partitionMetadata10 := PartitionMetadata{ID: 3, Leader: 1, Replicas: []int32{1}}
		partitionMetadata11 := PartitionMetadata{ID: 6, Leader: 4, Replicas: []int32{4}}
		partitionMetadata12 := PartitionMetadata{ID: 0, Leader: 4, Replicas: []int32{4}}
		topicMetadata := TopicMetadata{Name: "topic", Partitions: []*PartitionMetadata{&partitionMetadata1, &partitionMetadata2, &partitionMetadata3, &partitionMetadata4, &partitionMetadata5, &partitionMetadata6, &partitionMetadata7, &partitionMetadata8, &partitionMetadata9, &partitionMetadata10, &partitionMetadata11, &partitionMetadata12}}
		expectedJSONForReplicationFactor3 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 3, 4}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 6, 1}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 5, 6}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 2, 3}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 4, 5}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 1, 2}}}}
		expectedJSONForReplicationFactor4 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2, 3}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5, 6}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 4, 5, 1}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 1, 2, 4}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4, 5}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1, 2}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 6, 1, 3}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 3, 4, 6}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3, 4}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 5, 6, 2}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6, 1}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 2, 3, 5}}}}

		actualJSONForReplicationFactor3 := buildReassignmentJSON(topicMetadata, 3, 6)
		actualJSONForReplicationFactor4 := buildReassignmentJSON(topicMetadata, 4, 6)

		assert.Equal(t, expectedJSONForReplicationFactor3, actualJSONForReplicationFactor3)
		assert.Equal(t, expectedJSONForReplicationFactor4, actualJSONForReplicationFactor4)
	})
}
