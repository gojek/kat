package topic

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestDescribe(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	topics := []string{"topic1"}
	admin.On("DescribeTopics", mock.Anything).Return([]*sarama.TopicMetadata{}, nil).Times(1)

	describe(admin, topics)
	admin.AssertExpectations(t)
}
