package cmd

import (
	"testing"

	"github.com/gojekfarm/kat/pkg"
)

func TestShow(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	mockTopicCli.On("GetConfig", "topic1").Return([]pkg.ConfigEntry{}, nil).Times(1)
	mockTopicCli.On("GetConfig", "topic2").Return([]pkg.ConfigEntry{}, nil).Times(1)
	s := showConfig{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: topics}
	s.showConfig()
	mockTopicCli.AssertExpectations(t)
}
