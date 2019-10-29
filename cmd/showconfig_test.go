package cmd

import (
	"github.com/gojekfarm/kat/pkg"
	"testing"
)

func TestShow(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	TopicCli.(*pkg.MockTopicCli).On("ShowConfig", "topic1").Return([]pkg.ConfigEntry{}, nil).Times(1)
	TopicCli.(*pkg.MockTopicCli).On("ShowConfig", "topic2").Return([]pkg.ConfigEntry{}, nil).Times(1)
	s := showConfig{topics: topics}
	s.showConfig()
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	clearTopicCli(nil, nil)
}
