package topic

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"testing"
)

func TestList(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	admin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil).Times(1)
	l := list{admin: admin, replicationFactor: 1}
	l.List()
	admin.AssertExpectations(t)
}
