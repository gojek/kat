package config

import (
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/testutil"
	"testing"
)

func TestShow(t *testing.T) {
	admin := &testutil.MockClusterAdmin{}
	topics := []string{"topic1", "topic2"}
	admin.On("DescribeConfig", sarama.ConfigResource{Name: "topic1", Type: sarama.TopicResource}).Return([]sarama.ConfigEntry{}, nil).Times(1)
	admin.On("DescribeConfig", sarama.ConfigResource{Name: "topic2", Type: sarama.TopicResource}).Return([]sarama.ConfigEntry{}, nil).Times(1)

	show(admin, topics)
	admin.AssertExpectations(t)
}
