package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

type reassignPartitions struct {
	topics             string
	brokerIds          string
	zookeeper          string
	batch              int
	timeoutPerBatchInS int
	pollIntervalInS    int
	throttle           int
}

var reassignPartitionsCmd = &cobra.Command{
	Use:    "reassign-partitions",
	Short:  "Reassigns the partitions for topics",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		r := reassignPartitions{topics: Cobra.GetCmdArg("topics"), brokerIds: Cobra.GetCmdArg("broker-ids"),
			zookeeper: Cobra.GetCmdArg("zookeeper"), batch: Cobra.GetIntArg("batch"),
			timeoutPerBatchInS: Cobra.GetIntArg("timeout-per-batch"), pollIntervalInS: Cobra.GetIntArg("status-poll-interval"),
			throttle: Cobra.GetIntArg("throttle")}
		r.reassignPartitions()
	},
	PostRun: clearTopicCli,
}

func init() {
	reassignPartitionsCmd.PersistentFlags().StringP("topics", "t", "", "Regex to match the topics that require partition reassignment. eg: \".*\", \"test-.*-topic\", \"topic1|topic2\"")
	reassignPartitionsCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	reassignPartitionsCmd.PersistentFlags().StringP("broker-ids", "i", "", "Comma separated list of broker ids. eg: \"1,2,3,4,5,6\"")
	reassignPartitionsCmd.PersistentFlags().IntP("batch", "", 1, "Batch size to split reassignment")
	reassignPartitionsCmd.PersistentFlags().IntP("timeout-per-batch", "", 300, "Timeout for reassignment per batch in seconds")
	reassignPartitionsCmd.PersistentFlags().IntP("status-poll-interval", "", 5, "Interval in seconds for polling for reassignment status")
	reassignPartitionsCmd.PersistentFlags().IntP("throttle", "", 10000000, "Throttle for reassignment in bytes/sec")
	reassignPartitionsCmd.MarkPersistentFlagRequired("topics")
	reassignPartitionsCmd.MarkPersistentFlagRequired("zookeeper")
	reassignPartitionsCmd.MarkPersistentFlagRequired("broker-ids")
}

func (r *reassignPartitions) reassignPartitions() {
	topics, err := TopicCli.ListOnly(r.topics, true)
	if err != nil {
		fmt.Printf("Error while filtering topics - %v\n", err)
		return
	}

	if len(topics) == 0 {
		fmt.Printf("Did not find any topic matching - %v\n", r.topics)
		return
	}

	err = TopicCli.ReassignPartitions(topics, r.batch, r.timeoutPerBatchInS, r.pollIntervalInS, r.throttle, r.brokerIds, r.zookeeper)
	if err != nil {
		fmt.Println("Error while reassigning partitions:")
		fmt.Println(err)
		return
	}
	fmt.Println("Successfully reassigned partitions")
}
