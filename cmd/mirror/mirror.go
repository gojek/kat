package mirror

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/topicutil"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

type mirror struct {
	sourceAdmin      sarama.ClusterAdmin
	destinationAdmin sarama.ClusterAdmin
	topics           []string
	topicConfig      map[string]string
	createTopic      string
}

var MirrorCmd = &cobra.Command{
	Use:   "mirror",
	Short: "Mirror topic configurations from source to destination cluster",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		m := mirror{sourceAdmin: u.GetAdminClient("source-broker-ips"),
			destinationAdmin: u.GetAdminClient("destination-broker-ips"),
			topics:           u.GetTopicNames(),
			createTopic:      u.GetCmdArg("create-topics")}
		//TODO: Read configs to be mirrored from a json config file. Currently, everything is mirrored
		ok := m.getTopicConfigs()
		if !ok {
			return
		}
		m.alterTopicConfigs()
	},
}

func init() {
	MirrorCmd.PersistentFlags().StringP("source-broker-ips", "b", "", "Comma separated list of source broker ips")
	MirrorCmd.PersistentFlags().StringP("destination-broker-ips", "d", "", "Comma separated list of broker ips to mirror the configs to")
	MirrorCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topics to mirror the configs of. All topics are mirrored if not set.")
	//TODO: Mirror only the topics that have overridden configs.
	MirrorCmd.PersistentFlags().String("topics-with-overrides", "true", "Mirror only the topics that have overridden configs")
	MirrorCmd.PersistentFlags().String("create-topics", "false", "Create the topics on destination cluster if not present and mirror the configs")
	MirrorCmd.MarkPersistentFlagRequired("source-broker-ips")
	MirrorCmd.MarkPersistentFlagRequired("destination-broker-ips")
}

func (m *mirror) getTopicList() bool {
	if m.topics[0] == "" {
		//TODO: Take only the topics having overridden configs if `topics-with-overrides` flag is passed
		m.topics = topicutil.ListAll(m.sourceAdmin)
		if m.topics == nil {
			return false
		}
	}
	return true
}

func (m *mirror) getTopicConfigs() bool {
	ok := m.getTopicList()
	if !ok {
		return false
	}

	topicToConfig := make(map[string]string)
	for _, topic := range m.topics {
		configs := topicutil.DescribeConfig(m.sourceAdmin, topic)
		if configs == nil {
			return false
		}
		configStr := topicutil.ConfigString(configs, topic)
		topicToConfig[topic] = configStr
	}
	m.topicConfig = topicToConfig
	return true
}

func (m *mirror) alterTopicConfigs() {
	destinationClusterTopics := topicutil.ListAll(m.destinationAdmin)
	if destinationClusterTopics == nil {
		return
	}
	sourceTopicsDetails := topicutil.ListTopicDetails(m.sourceAdmin)
	for _, topic := range m.topics {
		if topicPresent(topic, destinationClusterTopics) == false {
			if m.createTopic == "true" {
				topicDetail := sourceTopicsDetails[topic]
				topicutil.CreateTopic(m.destinationAdmin, topic, &topicDetail, false)
				continue

			} else {
				fmt.Printf("Topic %s not present on destination cluster", topic)
				continue
			}
		}
		configMap := topicutil.ConfigMap(m.topicConfig[topic])
		err := m.destinationAdmin.AlterConfig(sarama.TopicResource, topic, configMap, false)
		if err != nil {
			fmt.Printf("Err while altering config for topic - %v: %v\n", topic, err)
			continue
		} else {
			fmt.Printf("Config was successfully altered for topic - %v\n", topic)
		}
	}
}

func topicPresent(topic string, destinationClusterTopics []string) bool {
	for _, destinationClustertopic := range destinationClusterTopics {
		if destinationClustertopic == topic {
			return true
		}
	}
	return false
}
