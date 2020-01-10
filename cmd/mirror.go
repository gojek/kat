package cmd

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/gojekfarm/kat/util"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
)

type mirror struct {
	sourceCli          pkg.TopicCli
	destinationCli     pkg.TopicCli
	createTopic        bool
	increasePartitions bool
	dryRun             bool
	excludeConfigs     []string
}

var mirrorCmd = &cobra.Command{
	Use:   "mirror",
	Short: "Mirror topic configurations from source to destination cluster",
	Run: func(cmd *cobra.Command, args []string) {
		cobraUtil := util.NewCobraUtil(cmd)

		sourceCli := Init(cobraUtil, WithAddr("source-broker-ips")).TopicCli
		destinationCli := Init(cobraUtil, WithAddr("destination-broker-ips")).TopicCli
		m := mirror{sourceCli: sourceCli,
			destinationCli:     destinationCli,
			createTopic:        cobraUtil.GetBoolArg("create-topics"),
			increasePartitions: cobraUtil.GetBoolArg("increase-partitions"),
			dryRun:             cobraUtil.GetBoolArg("dry-run"),
			excludeConfigs:     cobraUtil.GetStringSliceArg("exclude-configs"),
		}
		//TODO: Read configs to be mirrored from a json config file. Currently, everything is mirrored
		m.mirrorTopicConfigs()
	},
}

func init() {
	mirrorCmd.PersistentFlags().StringP("source-broker-ips", "b", "", "Comma separated list of source broker ips")
	mirrorCmd.PersistentFlags().StringP("destination-broker-ips", "d", "", "Comma separated list of broker ips to mirror the configs to")
	//TODO: Mirror only the topics that have overridden configs.
	mirrorCmd.PersistentFlags().String("topics-with-overrides", "true", "Mirror only the topics that have overridden configs")
	mirrorCmd.PersistentFlags().Bool("create-topics", false, "Create the topics on destination cluster if not present and mirror the configs")
	mirrorCmd.PersistentFlags().Bool("increase-partitions", false, "Increase the partition count of topics on destination cluster")
	mirrorCmd.MarkPersistentFlagRequired("source-broker-ips")
	mirrorCmd.MarkPersistentFlagRequired("destination-broker-ips")
	mirrorCmd.PersistentFlags().Bool("dry-run", false, "shows only the configs which gets updated")
	mirrorCmd.PersistentFlags().StringSlice("exclude-configs", []string{}, "Comma separated list of topics configs need to be excluded")
}

func (m *mirror) mirrorTopicConfigs() {
	sourceTopics, sourceTopicConfigs, err := getTopicDetailsAndConfigs(m.sourceCli)
	if err != nil {
		logger.Fatalf("Source cluster - %v\n", err)
	}

	destinationTopics, destinationTopicConfigs, err := getTopicDetailsAndConfigs(m.destinationCli)
	if err != nil {
		logger.Fatalf("Destination cluster - %v\n", err)
	}

	tw := &util.TableWriter{}
	for topic, detail := range sourceTopics {
		var err error
		if destinationTopics[topic].NumPartitions == 0 {
			if !m.createTopic {
				logger.Infof("Topic - %v does not exist in destination cluster. Pass --create-topics flag\n", topic)
				continue
			}
			if !m.dryRun {
				err = m.destinationCli.Create(topic, detail, false)
				if err != nil {
					logger.Errorf("Err while creating topic %v in destination cluster - %v\n", topic, err)
				}
			}
			tw.AddRow(util.MirrorStatus(topic, jsonString(detail.Config), detail.NumPartitions, detail.NumPartitions, true, m.dryRun, err))
		} else {
			sourceNumOfPartitions := sourceTopics[topic].NumPartitions
			destNumOfPartitions := destinationTopics[topic].NumPartitions
			sourceCM := getConfigMap(sourceTopicConfigs[topic], m.excludeConfigs)
			destinationCM := getConfigMap(destinationTopicConfigs[topic], m.excludeConfigs)
			equalConfigs := reflect.DeepEqual(destinationCM, sourceCM)

			if equalConfigs && (!m.increasePartitions || (sourceNumOfPartitions <= destNumOfPartitions)) {
				logger.Debugf("Configs are equal for topic - %v\n", topic)
				continue
			} else {
				changelogs, err := diff.Diff(destinationCM, sourceCM)
				if err != nil {
					logger.Errorf("Err while comparing configs for topic %v - %v\n", topic, err)
					continue
				}

				if !m.dryRun {
					err = m.updateTopicInDestinationCluster(topic, sourceNumOfPartitions, destNumOfPartitions, changelogs)
				}

				tw.AddRow(util.MirrorStatus(topic, fmt.Sprint(changelogs), destNumOfPartitions, sourceNumOfPartitions, false, m.dryRun, err))
			}
		}
	}

	tw.Render()
}

func (m *mirror) updateTopicInDestinationCluster(topic string, sourceNumOfPartitions int32, destNumOfPartitions int32, changelogs diff.Changelog) error {
	err := m.increasePartitionsIfEnabled(topic, sourceNumOfPartitions, destNumOfPartitions)
	if err != nil {
		logger.Errorf("Err while increasing partitions for topic %v - %v\n", topic, err)
		return err
	}

	if len(changelogs) == 0 {
		return nil
	}

	err = m.destinationCli.UpdateConfig([]string{topic}, configToUpdate(changelogs), false)
	if err != nil {
		logger.Errorf("Err while updating config for topic %v - %v\n", topic, err)
	}
	return err
}

func getTopicDetailsAndConfigs(cli pkg.TopicCli) (map[string]pkg.TopicDetail, map[string][]pkg.ConfigEntry, error) {
	topics, err := cli.List()
	if err != nil {
		return nil, nil, fmt.Errorf("err while fetching topics - %v", err)
	}
	if len(topics) == 0 {
		logger.Info("No topics found in cluster")
		return nil, nil, nil
	}

	topicConfigs := make(map[string][]pkg.ConfigEntry)
	for topic := range topics {
		entries, err := cli.GetConfig(topic)
		if err != nil {
			return nil, nil, fmt.Errorf("err while reading config for topic %v - %v", topic, err)
		}
		topicConfigs[topic] = entries
	}

	return topics, topicConfigs, nil
}

func (m *mirror) increasePartitionsIfEnabled(topic string, sourceNumOfPartitions, destNumOfPartitions int32) error {
	if sourceNumOfPartitions > destNumOfPartitions {
		if !m.increasePartitions {
			logger.Infof("Partitions are not the same for topic %v. Pass --increase-partitions flag", topic)
		} else {
			err := m.destinationCli.CreatePartitions(topic, sourceNumOfPartitions, [][]int32{}, false)
			if err != nil {
				return fmt.Errorf("err while increasing partitions for topic %v - %v", topic, err)
			}
		}
	}
	return nil
}

func getConfigMap(configList []pkg.ConfigEntry, excludeConfigs []string) map[string]string {
	configMap := make(map[string]string)
	for _, config := range configList {
		if util.Contains(excludeConfigs, config.Name) {
			continue
		}
		configMap[config.Name] = config.Value
	}
	return configMap
}

func configToUpdate(changelogs diff.Changelog) map[string]*string {
	result := make(map[string]*string)
	for _, log := range changelogs {
		val := fmt.Sprint(log.To)
		result[log.Path[0]] = &val
	}
	return result
}

func jsonString(configs map[string]*string) string {
	configJSON := make(map[string]string)
	for k, v := range configs {
		configJSON[k] = *v
	}

	outputJSON, _ := json.MarshalIndent(configJSON, "", "    ")
	return string(outputJSON)
}
