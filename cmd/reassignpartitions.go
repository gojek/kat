package cmd

import (
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

type reassignPartitions struct {
	BaseCmd
	topics             string
	brokerIds          string
	zookeeper          string
	batch              int
	timeoutPerBatchInS int
	pollIntervalInS    int
	throttle           int
}

var reassignPartitionsCmd = &cobra.Command{
	Use:   "reassign-partitions",
	Short: "Reassigns the partitions for topics",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := util.NewCobraUtil(command)
		baseCmd := Init(cobraUtil)
		r := reassignPartitions{BaseCmd: baseCmd, topics: cobraUtil.GetStringArg("topics"), brokerIds: cobraUtil.GetStringArg("broker-ids"),
			zookeeper: cobraUtil.GetStringArg("zookeeper"), batch: cobraUtil.GetIntArg("batch"),
			timeoutPerBatchInS: cobraUtil.GetIntArg("timeout-per-batch"), pollIntervalInS: cobraUtil.GetIntArg("status-poll-interval"),
			throttle: cobraUtil.GetIntArg("throttle")}
		r.reassignPartitions()
	},
}

func init() {
	reassignPartitionsCmd.PersistentFlags().StringP("topics", "t", "",
		"Regex to match the topics that require partition reassignment. eg: \".*\", \"test-.*-topic\", \"topic1|topic2\"")
	reassignPartitionsCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	reassignPartitionsCmd.PersistentFlags().StringP("broker-ids", "i", "", "Comma separated list of broker ids. eg: \"1,2,3,4,5,6\"")
	reassignPartitionsCmd.PersistentFlags().IntP("batch", "", 1, "Batch size to split reassignment")
	reassignPartitionsCmd.PersistentFlags().IntP("timeout-per-batch", "", 300, "Timeout for reassignment per batch in seconds")
	reassignPartitionsCmd.PersistentFlags().IntP("status-poll-interval", "", 5, "Interval in seconds for polling for reassignment status")
	reassignPartitionsCmd.PersistentFlags().IntP("throttle", "", 10000000, "Throttle for reassignment in bytes/sec")
	if err := reassignPartitionsCmd.MarkPersistentFlagRequired("topics"); err != nil {
		logger.Fatal(err)
	}
	if err := reassignPartitionsCmd.MarkPersistentFlagRequired("zookeeper"); err != nil {
		logger.Fatal(err)
	}
	if err := reassignPartitionsCmd.MarkPersistentFlagRequired("broker-ids"); err != nil {
		logger.Fatal(err)
	}
}

func (r *reassignPartitions) reassignPartitions() {
	topics, err := r.TopicCli.ListOnly(r.topics, true)
	if err != nil {
		logger.Fatalf("Error while filtering topics - %v\n", err)
	}

	if len(topics) == 0 {
		logger.Infof("Did not find any topic matching - %v\n", r.topics)
		return
	}

	err = r.TopicCli.ReassignPartitions(topics, r.batch, r.timeoutPerBatchInS, r.pollIntervalInS, r.throttle, r.brokerIds, r.zookeeper)
	if err != nil {
		logger.Errorf("Error while reassigning partitions: %s", err)
		return
	}
	logger.Info("Successfully reassigned partitions")
}
