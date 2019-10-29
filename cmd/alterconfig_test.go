package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestAlter(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	config := "key1=val1"
	value := "val1"
	configMap := map[string]*string{"key1": &value}
	TopicCli.(*pkg.MockTopicCli).On("UpdateConfig", topics, configMap, false).Return(nil).Times(1)
	a := alterConfig{topics: topics, config: config}
	a.alterConfig()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
