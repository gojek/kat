package client

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSaramaClient_ListTopicDetailsSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
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
	client := SaramaClient{admin: admin}
	expectedErr := errors.New("error")
	admin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, expectedErr)

	_, err := client.ListTopicDetails()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_DescribeTopicMetadataSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
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
	client := SaramaClient{admin: admin}
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
	client := SaramaClient{admin: admin}

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
	client := SaramaClient{admin: admin}
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
	client := SaramaClient{admin: admin}
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

	configEntries, err := client.GetConfig(configResource)
	assert.NoError(t, err)
	assert.Equal(t, expectedConfigEntries, configEntries)
	admin.AssertExpectations(t)
}

func TestSaramaClient_ShowConfigFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
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

	_, err := client.GetConfig(configResource)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_DeleteTopicSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
	topics := []string{"topic-1", "topic-2"}
	admin.On("DeleteTopic", mock.Anything).Return(nil)

	err := client.DeleteTopic(topics)

	assert.NoError(t, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_ListBrokersSuccess(t *testing.T) {
	saramaClient := &MockSaramaClient{}
	client := SaramaClient{client: saramaClient}
	saramaClient.On("Brokers").Return([]*sarama.Broker{sarama.NewBroker("abc:123")})

	brokers := client.ListBrokers()

	assert.Equal(t, map[int]string{-1: "abc:123"}, brokers)
	saramaClient.AssertExpectations(t)
}

func TestSaramaClient_CreateTopicSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
	topicName := "topic-1"
	detail := TopicDetail{NumPartitions: 5}
	adminDetail := &sarama.TopicDetail{NumPartitions: 5}
	validateOnly := false
	admin.On("CreateTopic", topicName, adminDetail, validateOnly).Return(nil)

	err := client.CreateTopic(topicName, detail, validateOnly)

	assert.NoError(t, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_CreateTopicFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
	topicName := "topic-1"
	detail := TopicDetail{NumPartitions: 5}
	adminDetail := &sarama.TopicDetail{NumPartitions: 5}
	validateOnly := false
	admin.On("CreateTopic", topicName, adminDetail, validateOnly).Return(errors.New("error"))

	err := client.CreateTopic(topicName, detail, validateOnly)

	assert.Error(t, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_CreatePartitionsSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
	topicName := "topic-1"
	count := int32(10)
	assignment := [][]int32{}
	validateOnly := false
	admin.On("CreatePartitions", topicName, count, assignment, validateOnly).Return(nil)

	err := client.CreatePartitions(topicName, count, assignment, validateOnly)

	assert.NoError(t, err)
	admin.AssertExpectations(t)
}

func TestSaramaClient_CreatePartitionsFailure(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}
	topicName := "topic-1"
	count := int32(10)
	assignment := [][]int32{}
	validateOnly := false
	admin.On("CreatePartitions", topicName, count, assignment, validateOnly).Return(errors.New("error"))

	err := client.CreatePartitions(topicName, count, assignment, validateOnly)

	assert.Error(t, err)
	admin.AssertExpectations(t)
}
func TestSaramaClient_GetConsumerGroupsForTopic(t *testing.T) {
	admin := &MockClusterAdmin{}
	client := SaramaClient{admin: admin}

	groupDesciption := []*sarama.GroupDescription{{
		GroupId: "test-group-id",
		Members: map[string]*sarama.GroupMemberDescription{
			"instance-id-0": {
				ClientId:         "instance-id-0",
				MemberAssignment: []byte{0x04, 0x05, 0x06},
			},

			"instance-id-1": {
				ClientId:         "instance-id-1",
				MemberAssignment: []byte{0x04, 0x05, 0x06},
			},

			"instance-id-2": {
				ClientId:         "instance-id-2",
				MemberAssignment: []byte{0x04, 0x05, 0x06},
			},
		},
	}}

	admin.On("DescribeConsumerGroups", []string{"test-group-id"}).Return(groupDesciption, nil)

	_, err := client.GetConsumerGroupsForTopic([]string{"test-group-id"}, "test-topic")

	require.NoError(t, err)
}

func TestSaramaClient_GetEmptyTopicsSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	mockClient := &MockSaramaClient{}
	client := SaramaClient{admin: admin, client: mockClient}
	brokerIDs := []int32{-1}
	brokers := []*sarama.Broker{sarama.NewBroker("broker-1:1234")}
	mockClient.On("Brokers").Return(brokers).Once()
	admin.On("DescribeLogDirs", brokerIDs).Return(nil, errors.New("sample error")).Once() //brokerid and response have different ids to check proper implementation
	logDirs, err := client.DescribeLogDirs(brokerIDs)
	assert.Nil(t, logDirs)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "sample error")
	admin.AssertExpectations(t)
}

func TestSaramaClient_GetLogDirsSuccess(t *testing.T) {
	admin := &MockClusterAdmin{}
	mockClient := &MockSaramaClient{}
	client := SaramaClient{admin: admin, client: mockClient}
	brokerIDs := []int32{-1}
	brokerMap := make(map[int32][]string)
	brokerMap[-1] = []string{"topic-1#1:0,2:0,4:0", "topic-2#2:0,4:0"}
	brokerMap[2] = []string{"topic-1#3:0", "topic-2#1:0,3:0"}
	expectedBrokerMetaData := getBrokerMetaData(brokerMap, nil)
	saramaBrokerMetaData := getSaramaBrokerMetaData(brokerMap, sarama.ErrNoError)
	brokers := []*sarama.Broker{sarama.NewBroker("broker-1:1234")}
	mockClient.On("Brokers").Return(brokers).Once()
	admin.On("DescribeLogDirs", brokerIDs).Return(saramaBrokerMetaData, nil).Once() //brokerid and response have different ids to check proper implementation
	logDirsMap, err := client.DescribeLogDirs(brokerIDs)
	assert.Nil(t, err)
	assert.Equal(t, expectedBrokerMetaData, logDirsMap)
	admin.AssertExpectations(t)
}

func TestSaramaClient_GetLogDirsSuccessErrorConversion(t *testing.T) {
	admin := &MockClusterAdmin{}
	mockClient := &MockSaramaClient{}
	client := SaramaClient{admin: admin, client: mockClient}
	brokerIDs := []int32{-1}
	brokerMap := make(map[int32][]string)
	brokerMap[-1] = []string{"topic-1#1:0,2:0,4:0", "topic-2#2:0,4:0"}
	brokerMap[2] = []string{"topic-1#3:0", "topic-2#1:0,3:0"}
	saramaBrokerMetaData := getSaramaBrokerMetaData(brokerMap, sarama.ErrBrokerNotAvailable)
	brokers := []*sarama.Broker{sarama.NewBroker("broker-1:1234")}
	mockClient.On("Brokers").Return(brokers).Once()
	admin.On("DescribeLogDirs", brokerIDs).Return(saramaBrokerMetaData, nil).Once() //brokerid and response have different ids to check proper implementation
	logDirsMap, err := client.DescribeLogDirs(brokerIDs)
	assert.Nil(t, err)
	assert.NotNil(t, logDirsMap[-1][0].Error)
	admin.AssertExpectations(t)
}

func getTopicPartitions(topic string, partitions []string) DescribeLogDirsResponseTopic {
	list := make([]DescribeLogDirsResponsePartition, 0, len(partitions))
	for _, val := range partitions {
		splitStrings := strings.Split(val, ":")
		id, _ := strconv.ParseInt(splitStrings[0], 10, 32)
		size, _ := strconv.ParseInt(splitStrings[1], 10, 64)
		list = append(list, DescribeLogDirsResponsePartition{PartitionID: int32(id), Size: size})
	}
	return DescribeLogDirsResponseTopic{Topic: topic, Partitions: list}
}

func getSaramaTopicPartitions(topic string, partitions []string) sarama.DescribeLogDirsResponseTopic {
	list := make([]sarama.DescribeLogDirsResponsePartition, 0, len(partitions))
	for _, val := range partitions {
		splitStrings := strings.Split(val, ":")
		id, _ := strconv.ParseInt(splitStrings[0], 10, 32)
		size, _ := strconv.ParseInt(splitStrings[1], 10, 64)
		list = append(list, sarama.DescribeLogDirsResponsePartition{PartitionID: int32(id), Size: size})
	}
	return sarama.DescribeLogDirsResponseTopic{Topic: topic, Partitions: list}
}

func getBrokerMetaData(configMap map[int32][]string, err error) map[int32][]DescribeLogDirsResponseDirMetadata {
	brokerMap := make(map[int32][]DescribeLogDirsResponseDirMetadata, len(configMap))
	for brokerID, configList := range configMap {
		topicList := make([]DescribeLogDirsResponseTopic, 0, len(configList))
		for _, conf := range configList {
			t1 := strings.Split(conf, "#")
			p := strings.Split(t1[1], ",")
			topic := getTopicPartitions(t1[0], p)
			topicList = append(topicList, topic)
		}
		brokerList := []DescribeLogDirsResponseDirMetadata{{Topics: topicList, Error: err}}
		brokerMap[brokerID] = brokerList
	}
	return brokerMap
}

func getSaramaBrokerMetaData(configMap map[int32][]string, err sarama.KError) map[int32][]sarama.DescribeLogDirsResponseDirMetadata {
	brokerMap := make(map[int32][]sarama.DescribeLogDirsResponseDirMetadata, len(configMap))
	for brokerID, configList := range configMap {
		topicList := make([]sarama.DescribeLogDirsResponseTopic, 0, len(configList))
		for _, conf := range configList {
			t1 := strings.Split(conf, "#")
			p := strings.Split(t1[1], ",")
			topic := getSaramaTopicPartitions(t1[0], p)
			topicList = append(topicList, topic)
		}
		brokerList := []sarama.DescribeLogDirsResponseDirMetadata{{Topics: topicList, ErrorCode: err}}
		brokerMap[brokerID] = brokerList
	}
	return brokerMap
}
