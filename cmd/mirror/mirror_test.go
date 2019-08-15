package mirror

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestTopicMirroredIfTopicExistsInDestination(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetails := make(map[string]sarama.TopicDetail)
	topicDetails["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("ListTopics").Return(topicDetails, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}}
	m.alterTopicConfigs()
	destinationAdmin.AssertExpectations(t)
}

func TestTopicIsNotMirroredIfTopicDoesNotExistsInDestination(t *testing.T) {
	sourceAdmin := &testutil.MockClusterAdmin{}
	destinationAdmin := &testutil.MockClusterAdmin{}
	topicDetailsSrc := make(map[string]sarama.TopicDetail)
	topicDetailsDest := make(map[string]sarama.TopicDetail)
	topicDetailsSrc["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	topicDetailsDest["topic2"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	sourceAdmin.On("ListTopics").Return(topicDetailsSrc, nil)
	destinationAdmin.On("ListTopics").Return(topicDetailsDest, nil)
	destinationAdmin.On("AlterConfig", sarama.TopicResource, "topic1", mock.AnythingOfType("map[string]*string"), false).Return(nil)
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, createTopic: "false"}
	m.alterTopicConfigs()
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
	m := mirror{sourceAdmin: sourceAdmin, destinationAdmin: destinationAdmin, topics: []string{"topic1"}, topicConfig: map[string]string{"topic1": "retention.ms=200000"}, createTopic: "true"}
	m.alterTopicConfigs()
	destinationAdmin.AssertExpectations(t)
	sourceAdmin.AssertExpectations(t)

}
