package config

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"

	"github.com/gojekfarm/kat/pkg/client"

	"github.com/gojekfarm/kat/logger"
)

func init() {
	logger.SetDummyLogger()
}

func TestShow_Success(t *testing.T) {
	mockConfigurer := &client.MockConfigurer{}
	topics := []string{"topic1", "topic2"}
	mockConfigurer.On("GetConfig", "topic1").Return([]client.ConfigEntry{}, nil).Times(1)
	mockConfigurer.On("GetConfig", "topic2").Return([]client.ConfigEntry{}, nil).Times(1)
	s := showConfig{Configurer: mockConfigurer, topics: topics}
	s.showConfig()
	mockConfigurer.AssertExpectations(t)
}

func TestShow_Failure(t *testing.T) {
	mockConfigurer := &client.MockConfigurer{}
	topics := []string{"topic1", "topic2"}
	mockConfigurer.On("GetConfig", "topic1").Return([]client.ConfigEntry{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	s := showConfig{Configurer: mockConfigurer, topics: topics}
	assert.PanicsWithValue(t, "os.Exit called", s.showConfig, "os.Exit was not called")
	mockConfigurer.AssertExpectations(t)
}
