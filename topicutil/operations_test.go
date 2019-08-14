package topicutil

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestListAll(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	topicDetails := make(map[string]sarama.TopicDetail)
	topicDetails["topic1"] = sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 1}
	admin.On("ListTopics").Return(topicDetails, nil)
	topics := ListAll(admin)
	assert.Equal(t, "topic1", topics[0])
	admin.AssertExpectations(t)
}

func TestDescribeTopicMetadata(t *testing.T) {
	metadata := make([]*sarama.TopicMetadata, 1)
	topics := []string{"topic1"}
	admin := &testutil.MockClusterAdmin{}
	admin.On("DescribeTopics", topics).Return(metadata, nil)
	metadata[0] = &sarama.TopicMetadata{Name: "topic1"}
	m := DescribeTopicMetadata(admin, []string{"topic1"})
	assert.Equal(t, "topic1", m[0].Name)
	admin.AssertExpectations(t)
}

func TestDescribeConfig(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	configs := make([]sarama.ConfigEntry, 2)
	configs[0] = sarama.ConfigEntry{Name: "retention.ms", Value: "120000"}
	configs[1] = sarama.ConfigEntry{Name: "cleanup.policy", Value: "compact"}
	admin.On("DescribeConfig", mock.AnythingOfType("sarama.ConfigResource")).Return(configs, nil)
	cfg := DescribeConfig(admin, "topic1")
	assert.Equal(t, "retention.ms", cfg[0].Name)
	assert.Equal(t, "120000", cfg[0].Value)
	assert.Equal(t, "cleanup.policy", cfg[1].Name)
	admin.AssertExpectations(t)
}
