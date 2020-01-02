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
	"strings"
)

type mirror struct {
	sourceAdmin      sarama.ClusterAdmin
	destinationAdmin sarama.ClusterAdmin
	topics           []string
	topicConfig      map[string]string
	createTopic      bool
	partitions       bool
	dryRun           bool
	excludeConfigs   map[string]interface{}
}

var MirrorCmd = &cobra.Command{
	Use:   "mirror",
	Short: "Mirror topic configurations from source to destination cluster",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		sourceAdmin, _ := u.GetSaramaClient("source-broker-ips")
		destinationAdmin, _ := u.GetSaramaClient("destination-broker-ips")
		m := mirror{sourceAdmin: sourceAdmin,
			destinationAdmin: destinationAdmin,
			topics:           u.GetTopicNames(),
			createTopic:      u.GetBoolArg("create-topics"),
			partitions:       u.GetBoolArg("increase-partitions"),
			dryRun:           u.GetBoolArg("dry-run"),
			excludeConfigs:   u.GetStringSet("exclude-configs"),
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
	MirrorCmd.PersistentFlags().StringSlice("exclude-configs", []string{}, "Comma separated list of topics configs need to be excluded")
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
		configs := topicutil.DescribeFilteredConfig(m.sourceAdmin, topic, m.excludeConfigs)
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

		equal, changeLogs := m.topicConfigEquals(topic)
		if !equal {
			topicUpdateOutput := m.alterTopicConfigs(topic, changeLogs, sourceTopicsDetails[topic].NumPartitions, destinationTopicDetails[topic].NumPartitions, m.dryRun)
			output = append(output, topicUpdateOutput...)
		}

	}

	table := tablewriter.NewWriter(os.Stdout)
	if len(output) > 0 {
		table.SetHeader(output[0].Headers())
		for _, v := range output {
			table.Append(v.Row())
		}
		table.Render()
	}
}

func (m *mirror) alterTopicConfigs(topic string, diff diff.Changelog, oldNoOfPartitions int32, newNoOfPartitions int32, dryRun bool) []topicutil.Output {
	configMap := topicutil.ConfigMap(m.topicConfig[topic])
	if dryRun {
		return []topicutil.Output{{Topic: topic, Action: topicutil.Update, ConfigChange: fmt.Sprint(diff), OldPartitionCount: oldNoOfPartitions, NewPartitionCount: newNoOfPartitions, Status: topicutil.DryRun}}
	}

	errList := []string{}
	errIncreasingPartition := false
	if m.partitions {
		err := m.increasePartitions(topic, oldNoOfPartitions, newNoOfPartitions, m.dryRun)
		if err != nil {
			errList = append(errList, err.Error())
			errIncreasingPartition = true
		}
	}

	errChangingConfig := false
	err := m.destinationAdmin.AlterConfig(sarama.TopicResource, topic, configMap, false)
	if err != nil {
		errChangingConfig = true
		errList = append(errList, err.Error())
	}

	actualNewPartitionCount := newNoOfPartitions
	if errIncreasingPartition {
		actualNewPartitionCount = oldNoOfPartitions
	}

	status := topicutil.Success
	if errChangingConfig || errIncreasingPartition {
		status = topicutil.Failure
	}

	errString := strings.Join(errList, ",")
	return []topicutil.Output{{Topic: topic, Action: topicutil.Update, ConfigChange: fmt.Sprint(diff), OldPartitionCount: oldNoOfPartitions, NewPartitionCount: actualNewPartitionCount, Status: status, Reason: errString}}

}

func (m *mirror) increasePartitions(topic string, srcNoOfPartitions int32, destNoOfPartitions int32, dryRun bool) error {
	if srcNoOfPartitions == destNoOfPartitions {
		return nil
	}
	return m.destinationAdmin.CreatePartitions(topic, srcNoOfPartitions, [][]int32{}, false)
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
	sourceTopicConfigMap := m.getConfigMap(topicutil.DescribeFilteredConfig(m.sourceAdmin, topic, m.excludeConfigs))
	destinationTopicConfigMap := m.getConfigMap(topicutil.DescribeFilteredConfig(m.destinationAdmin, topic, m.excludeConfigs))
	isConfigSame := reflect.DeepEqual(sourceTopicConfigMap, destinationTopicConfigMap)
	changeLog, _ := diff.Diff(destinationTopicConfigMap, sourceTopicConfigMap)
	return isConfigSame, changeLog
}

func (m *mirror) getConfigMap(configList []sarama.ConfigEntry) map[string]string {
	configMap := make(map[string]string)
	for _, config := range configList {
		configMap[config.Name] = config.Value
	}
	return configMap
}
