package model

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/gojek/kat/pkg/client"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withMockSSHClient(m *client.MockSSHClient) TopicOpts {
	return func(t *Topic) error {
		t.sshClient = m
		return nil
	}
}

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

func TestTopic_ListSizeLessThanEqualToSuccess(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	size := int64(0)
	brokerMetaDataConfig := map[int32][]string{
		-1: {"topic-1#1:0,2:0,3:0", "topic-2#4:0,5:0,6:0"},
		2:  {"topic-1#4:0,5:0,6:0", "topic-2#1:0,2:0,3:1"},
	}
	brokers := map[int]string{-1: "broker-1", 2: "broker-2"}
	metaData := getBrokerMetaData(brokerMetaDataConfig, nil)
	kafkaClient.On("ListBrokers").Return(brokers, nil).Twice()
	kafkaClient.On("DescribeLogDirs", []int32{-1, 2}).Return(metaData, nil).Twice()

	responseTopics, err := topicCli.ListTopicWithSizeLessThanOrEqualTo(size)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"topic-1"}, responseTopics)

	responseTopics2, err2 := topicCli.ListTopicWithSizeLessThanOrEqualTo(100)

	require.NoError(t, err2)
	assert.ElementsMatch(t, []string{"topic-1", "topic-2"}, responseTopics2)
	kafkaClient.AssertExpectations(t)
}

func TestTopic_ListSizeLessThanEqualToFailure(t *testing.T) {
	kafkaClient := &client.MockKafkaAPIClient{}
	topicCli, _ := NewTopic(kafkaClient)
	brokerMetaDataConfig := map[int32][]string{
		-1: {"topic-1#1:0,2:0,3:0", "topic-2#4:0,5:0,6:0"},
		2:  {"topic-1#4:0,5:0,6:0", "topic-2#1:0,2:0,3:1"},
	}
	sampleErr := errors.New("sample error")
	brokers := map[int]string{-1: "broker-1", 2: "broker-2"}
	metaData := getBrokerMetaData(brokerMetaDataConfig, sampleErr)
	kafkaClient.On("ListBrokers").Return(brokers, nil).Once()
	kafkaClient.On("DescribeLogDirs", []int32{-1, 2}).Return(metaData, nil).Once()

	responseTopics, err := topicCli.ListTopicWithSizeLessThanOrEqualTo(0)

	require.Error(t, err)
	assert.EqualError(t, err, sampleErr.Error())
	assert.Nil(t, responseTopics)
	kafkaClient.AssertExpectations(t)
}

func getTopicPartitions(topic string, partitions []string) client.DescribeLogDirsResponseTopic {
	list := make([]client.DescribeLogDirsResponsePartition, 0, len(partitions))
	for _, val := range partitions {
		splitStrings := strings.Split(val, ":")
		id, _ := strconv.ParseInt(splitStrings[0], 10, 32)
		size, _ := strconv.ParseInt(splitStrings[1], 10, 64)
		list = append(list, client.DescribeLogDirsResponsePartition{PartitionID: int32(id), Size: size})
	}
	return client.DescribeLogDirsResponseTopic{Topic: topic, Partitions: list}
}

func getBrokerMetaData(configMap map[int32][]string, err error) map[int32][]client.DescribeLogDirsResponseDirMetadata {
	brokerMap := make(map[int32][]client.DescribeLogDirsResponseDirMetadata, len(configMap))
	for brokerID, configList := range configMap {
		topicList := make([]client.DescribeLogDirsResponseTopic, 0, len(configList))
		for _, conf := range configList {
			t1 := strings.Split(conf, "#")
			p := strings.Split(t1[1], ",")
			topic := getTopicPartitions(t1[0], p)
			topicList = append(topicList, topic)
		}
		brokerList := []client.DescribeLogDirsResponseDirMetadata{{Topics: topicList, Error: err}}
		brokerMap[brokerID] = brokerList
	}
	return brokerMap
}
