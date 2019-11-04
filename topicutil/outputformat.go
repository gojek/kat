package topicutil

import (
	"encoding/json"
	"fmt"
)

type Output struct {
	Topic             string
	Action            Action
	ConfigChange      string
	OldPartitionCount int32
	NewPartitionCount int32
	Status            Status
	Reason            string
}

func (o Output) Row() []string {
	return []string{o.Topic, o.Action.String(), o.ConfigChange, fmt.Sprint(o.OldPartitionCount), fmt.Sprint(o.NewPartitionCount), o.Status.String(), o.Reason}
}

func (o Output) Headers() []string {
	return []string{"Topic", "Action", "Configs", "OldPartitionCount", "NewPartitionCount", "Status", "Reason"}
}

func toConfigJSON(configs map[string]*string) string {
	configJSON := make(map[string]string)
	for k, v := range configs {
		configJSON[k] = *v
	}

	outputJSON, _ := json.MarshalIndent(configJSON, "", "    ")
	return string(outputJSON)
}

type Action int

const (
	Create Action = iota
	Update
)

func (s Action) String() string {
	return [...]string{"Create", "Update"}[s]
}

type Status int

const (
	DryRun Status = iota
	Success
	Failure
)

func (s Status) String() string {
	return [...]string{"DryRun", "Success", "Failure"}[s]
}
