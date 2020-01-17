package config

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg/client"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetDummyLogger()
}

func TestAlter_Success(t *testing.T) {
	mockConfigurer := &client.MockConfigurer{}
	topics := []string{"topic1", "topic2"}
	config := "key1=val1"
	value := "val1"
	configMap := map[string]*string{"key1": &value}
	mockConfigurer.On("UpdateConfig", topics, configMap, false).Return(nil).Times(1)
	a := alterConfig{Configurer: mockConfigurer, topics: topics, config: config}
	a.alterConfig()
	mockConfigurer.AssertExpectations(t)
}

func TestAlter_Failure(t *testing.T) {
	mockConfigurer := &client.MockConfigurer{}
	topics := []string{"topic1", "topic2"}
	config := "key1=val1"
	value := "val1"
	configMap := map[string]*string{"key1": &value}
	mockConfigurer.On("UpdateConfig", topics, configMap, false).Return(errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	a := alterConfig{Configurer: mockConfigurer, topics: topics, config: config}
	assert.PanicsWithValue(t, "os.Exit called", a.alterConfig, "os.Exit was not called")
	mockConfigurer.AssertExpectations(t)
}
