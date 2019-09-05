package topic

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"github.com/stretchr/testify/mock"
)

func TestDescribe(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	topics := []string{"topic1"}
	admin.On("DescribeTopics", mock.Anything).Return([]*sarama.TopicMetadata{}, nil).Times(1)
	d := describe{admin: admin, topics: topics}
	d.describe()
	admin.AssertExpectations(t)
}
