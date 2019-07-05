package topic

import (
	"github.com/Shopify/sarama"
	"source.golabs.io/hermes/kafka-admin-tools/testutil"
	"testing"
)

func TestList(t *testing.T) {
	admin := testutil.MockClusterAdmin{}
	replicationFactor := 1
	admin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil).Times(1)

	list(&admin, replicationFactor)
	admin.AssertExpectations(t)
}
