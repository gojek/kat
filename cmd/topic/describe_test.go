package topic

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	"source.golabs.io/hermes/kafka-admin-tools/testutil"
	"testing"
)

func TestDescribe(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	topics := []string{"topic1"}
	admin.On("DescribeTopics", mock.Anything).Return([]*sarama.TopicMetadata{}, nil).Times(1)

	describe(admin, topics)
	admin.AssertExpectations(t)
}
