package topic

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"testing"
)

func TestList(t *testing.T) {
	admin := testutil.MockClusterAdmin{}
	replicationFactor := 1
	admin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil).Times(1)

	list(&admin, replicationFactor)
	admin.AssertExpectations(t)
}
