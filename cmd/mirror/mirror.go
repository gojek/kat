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
	createTopic      bool
	partitions       bool
}

var MirrorCmd = &cobra.Command{
	Use:   "mirror",
	Short: "Mirror topic configurations from source to destination cluster",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		m := mirror{sourceAdmin: u.GetAdminClient("source-broker-ips"),
			destinationAdmin: u.GetAdminClient("destination-broker-ips"),
			topics:           u.GetTopicNames(),
			createTopic:      u.GetBoolArg("create-topics"),
			partitions:       u.GetBoolArg("increase-partitions")}
		//TODO: Read configs to be mirrored from a json config file. Currently, everything is mirrored
		ok := m.getTopicConfigs()
		if !ok {
			return
		}
		m.mirrorTopicConfigs()
	},
}

func init() {
	MirrorCmd.PersistentFlags().StringP("source-broker-ips", "b", "", "Comma separated list of source broker ips")
	MirrorCmd.PersistentFlags().StringP("destination-broker-ips", "d", "", "Comma separated list of broker ips to mirror the configs to")
	MirrorCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topics to mirror the configs of. All topics are mirrored if not set.")
	//TODO: Mirror only the topics that have overridden configs.
	MirrorCmd.PersistentFlags().String("topics-with-overrides", "true", "Mirror only the topics that have overridden configs")
	MirrorCmd.PersistentFlags().Bool("create-topics", false, "Create the topics on destination cluster if not present and mirror the configs")
	MirrorCmd.PersistentFlags().Bool("increase-partitions", false, "Increase the partition count of topics on destination cluster")
	MirrorCmd.MarkPersistentFlagRequired("source-broker-ips")
	MirrorCmd.MarkPersistentFlagRequired("destination-broker-ips")
}

func (m *mirror) getTopicList() bool {
	if m.topics[0] == "" {
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

func (m *mirror) mirrorTopicConfigs() {
	destinationClusterTopics := topicutil.ListAll(m.destinationAdmin)
	destinationTopicDetails := topicutil.ListTopicDetails(m.destinationAdmin)
	sourceTopicsDetails := topicutil.ListTopicDetails(m.sourceAdmin)
	if destinationClusterTopics == nil || destinationTopicDetails == nil || sourceTopicsDetails == nil {
		return
	}
	for _, topic := range m.topics {
		if topicPresent(topic, destinationClusterTopics) == false {
			if m.createTopic {
				topicDetail := sourceTopicsDetails[topic]
				topicDetail.ReplicaAssignment = nil
				topicutil.CreateTopic(m.destinationAdmin, topic, &topicDetail, false)
				continue

			} else {
				fmt.Printf("Topic %s is not present on destination cluster. Pass --create-topics flag \n", topic)
				continue
			}
		}
		m.alterTopicConfigs(topic)
		if m.partitions {
			m.increasePartitions(topic, sourceTopicsDetails[topic].NumPartitions, destinationTopicDetails[topic].NumPartitions)
		}
	}
}

func (m *mirror) alterTopicConfigs(topic string) {
	configMap := topicutil.ConfigMap(m.topicConfig[topic])
	err := m.destinationAdmin.AlterConfig(sarama.TopicResource, topic, configMap, false)
	if err != nil {
		fmt.Printf("Err while altering config for topic - %v: %v\n", topic, err)
	} else {
		fmt.Printf("Config was successfully altered for topic - %v\n", topic)
	}
}

func (m *mirror) increasePartitions(topic string, srcNoOfPartitions int32, destNoOfPartitions int32) {
	if srcNoOfPartitions == destNoOfPartitions {
		fmt.Printf("Partition count not increased as it is same on both clusters \n")
		return
	}
	err := m.destinationAdmin.CreatePartitions(topic, srcNoOfPartitions, [][]int32{}, false)
	if err != nil {
		fmt.Printf("Err while increasing partition count for topic - %v: %v\n", topic, err)
	} else {
		fmt.Printf("Partition count successfully increased for topic - %v\n", topic)
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
