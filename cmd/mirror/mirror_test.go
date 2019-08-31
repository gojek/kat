package mirror

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestTopicConfigMirroredIfTopicExistsInDestination(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetails := make(map[string]sarama.TopicDetail)
	topicDetails["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, partitions: "false"}
	m.mirrorTopicConfigs()
	destinationAdmin.AssertExpectations(t)
}

func TestTopicConfigIsNotMirroredIfTopicDoesNotExistsInDestination(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetailsSrc := make(map[string]sarama.TopicDetail)
	topicDetailsDest := make(map[string]sarama.TopicDetail)
	topicDetailsSrc["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	topicDetailsDest["topic2"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetailsSrc, nil)
	destinationAdmin.On("ListTopics").Return(topicDetailsDest, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, createTopic: "false", partitions: "false"}
	m.mirrorTopicConfigs()
	destinationAdmin.AssertNotCalled(t, "AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false)
}

func TestTopicCreatedAndConfigMirrored(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetailsSrc := make(map[string]sarama.TopicDetail)
	topicDetailsDest := make(map[string]sarama.TopicDetail)
	topicDetailsSrc["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	topicDetailsDest["topic2"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	detail := topicDetailsSrc["topic1"]
	sourceAdmin.On("ListTopics").Return(topicDetailsSrc, nil)
	destinationAdmin.On("ListTopics").Return(topicDetailsDest, nil)
	destinationAdmin.On("CreateTopic", "topic1", &detail, false).Return(nil).Times(1)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, createTopic: "true", partitions: "false"}
	m.mirrorTopicConfigs()
	destinationAdmin.AssertExpectations(t)
	sourceAdmin.AssertExpectations(t)
}

func TestTopicPartitionCountNotMirrored(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetails := make(map[string]sarama.TopicDetail)
	topicDetails["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, partitions: "true"}
	m.mirrorTopicConfigs()
	destinationAdmin.AssertNotCalled(t, "CreatePartitions", "topic1", 2, nil, false)
	sourceAdmin.AssertExpectations(t)
	destinationAdmin.AssertExpectations(t)
}

func TestTopicPartitionCountMirrored(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetailsSrc := make(map[string]sarama.TopicDetail)
	topicDetailsDest := make(map[string]sarama.TopicDetail)
	cfgMap := make(map[string]*string)
	retention := "200000"
	cfgMap["retention.ms"] = &retention
	topicDetailsSrc["topic1"] = sarama.TopicDetail{NumPartitions: int32(10), ReplicationFactor: 2}
	topicDetailsDest["topic1"] = sarama.TopicDetail{NumPartitions: int32(6), ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetailsSrc, nil)
	destinationAdmin.On("ListTopics").Return(topicDetailsDest, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", cfgMap, false).Return(nil)
	destinationAdmin.On("CreatePartitions", "topic1", int32(10), [][]int32{}, false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, partitions: "true"}
	m.mirrorTopicConfigs()
	destinationAdmin.AssertExpectations(t)
	sourceAdmin.AssertExpectations(t)
}
