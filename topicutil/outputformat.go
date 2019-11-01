package topicutil

import "encoding/json"

type Output struct {
	Topic        string
	Action       Action
	ConfigChange string
	Status       Status
}

func (o Output) Row() []string {
	return []string{o.Topic, o.Action.String(), o.ConfigChange, o.Status.String()}
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
