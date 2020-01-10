package cmd

import (
	"testing"

	"github.com/gojekfarm/kat/pkg"
)

func TestDescribe(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	topics := []string{"topic1"}
	mockTopicCli.On("Describe", topics).Return([]*pkg.TopicMetadata{}, nil).Times(1)
	d := describeTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: topics}
	d.describeTopic()
	mockTopicCli.AssertExpectations(t)
}
