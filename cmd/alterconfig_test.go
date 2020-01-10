package cmd

import (
	"testing"

	"github.com/gojekfarm/kat/pkg"
)

func TestAlter(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	config := "key1=val1"
	value := "val1"
	configMap := map[string]*string{"key1": &value}
	mockTopicCli.On("UpdateConfig", topics, configMap, false).Return(nil).Times(1)
	a := alterConfig{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: topics, config: config}
	a.alterConfig()
	mockTopicCli.AssertExpectations(t)
}
