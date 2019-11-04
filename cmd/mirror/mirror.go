package mirror

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/topicutil"
	"github.com/gojekfarm/kat/util"
	"github.com/olekukonko/tablewriter"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
	"os"
	"reflect"
)

type mirror struct {
	sourceAdmin      sarama.ClusterAdmin
	destinationAdmin sarama.ClusterAdmin
	topics           []string
	topicConfig      map[string]string
	createTopic      bool
	partitions       bool
	dryRun           bool
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
			partitions:       u.GetBoolArg("increase-partitions"),
			dryRun:           u.GetBoolArg("dry-run"),
		}
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
	MirrorCmd.PersistentFlags().Bool("dry-run", false, "shows only the configs which gets updated")
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
	output := make([]topicutil.Output, 0)
	destinationClusterTopics := topicutil.ListAll(m.destinationAdmin)
	destinationTopicDetails := topicutil.ListTopicDetails(m.destinationAdmin)
	sourceTopicsDetails := topicutil.ListTopicDetails(m.sourceAdmin)
	if sourceTopicsDetails == nil {
		return
	}
	for _, topic := range m.topics {
		if topicPresent(topic, destinationClusterTopics) == false {
			if m.createTopic {
				topicDetail := sourceTopicsDetails[topic]
				topicDetail.ReplicaAssignment = nil
				topicCreateOutput := topicutil.CreateTopic(m.destinationAdmin, topic, &topicDetail, false, m.dryRun)
				output = append(output, topicCreateOutput...)
				continue

			} else {
				fmt.Printf("Topic %s is not present on destination cluster. Pass --create-topics flag \n", topic)
				continue
			}
		}

		equal, diff := m.topicConfigEquals(topic)
		if !equal {
			topicUpdateOutput := m.alterTopicConfigs(topic, diff, m.dryRun)
			output = append(output, topicUpdateOutput...)
		}
		if m.partitions {
			m.increasePartitions(topic, sourceTopicsDetails[topic].NumPartitions, destinationTopicDetails[topic].NumPartitions)
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Topic", "Action", "Configs", "Status"})

	for _, v := range output {
		table.Append(v.Row())
	}
	table.Render()
}

func (m *mirror) alterTopicConfigs(topic string, diff diff.Changelog, dryRun bool) []topicutil.Output {
	configMap := topicutil.ConfigMap(m.topicConfig[topic])
	if dryRun {
		return []topicutil.Output{{Topic: topic, Action: topicutil.Update, ConfigChange: fmt.Sprint(diff), Status: topicutil.DryRun}}
	}

	err := m.destinationAdmin.AlterConfig(sarama.TopicResource, topic, configMap, false)
	if err != nil {
		return []topicutil.Output{{Topic: topic, Action: topicutil.Update, ConfigChange: fmt.Sprint(diff), Status: topicutil.Failure}}
	}
	return []topicutil.Output{{Topic: topic, Action: topicutil.Update, ConfigChange: fmt.Sprint(diff), Status: topicutil.Success}}

}

func (m *mirror) increasePartitions(topic string, srcNoOfPartitions int32, destNoOfPartitions int32) {
	if srcNoOfPartitions == destNoOfPartitions {
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

func (m *mirror) topicConfigEquals(topic string) (bool, diff.Changelog) {
	sourceTopicConfigMap := m.getConfigMap(topicutil.DescribeConfig(m.sourceAdmin, topic))
	destinationTopicConfigMap := m.getConfigMap(topicutil.DescribeConfig(m.destinationAdmin, topic))
	isConfigSame := reflect.DeepEqual(sourceTopicConfigMap, destinationTopicConfigMap)
	changeLog, _ := diff.Diff(sourceTopicConfigMap, destinationTopicConfigMap)
	return isConfigSame, changeLog
}

func (m *mirror) getConfigMap(configList []sarama.ConfigEntry) map[string]string {
	configMap := make(map[string]string)
	for _, config := range configList {
		configMap[config.Name] = config.Value
	}
	return configMap
}
