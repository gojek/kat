package list

import (
	"sort"
	"strings"

	"github.com/gojek/kat/cmd/base"
	"github.com/gojek/kat/logger"
	"github.com/gojek/kat/pkg/client"
	"github.com/spf13/cobra"
)

type consumerGroupAdmin struct {
	saramaClient client.ConsumerLister
}

var ListConsumerGroupsCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the consumer groups",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)

		addr := strings.Split(cobraUtil.GetStringArg("broker-list"), ",")

		cgl := consumerGroupAdmin{
			saramaClient: client.NewSaramaClient(addr),
		}
		err := cgl.ListGroups(cobraUtil.GetStringArg("topic"))
		if err != nil {
			logger.Fatalf("Error while listing consumer groups for %v topic", err)
		}
	},
}

func (c *consumerGroupAdmin) ListGroups(topic string) error {
	consumerGroupsMap, err := c.saramaClient.ListConsumerGroups()
	if err != nil {
		return err
	}

	consumerGroupList := make([]string, len(consumerGroupsMap))
	for group := range consumerGroupsMap {
		consumerGroupList = append(consumerGroupList, group)
	}

	sort.Slice(consumerGroupList, func(i int, j int) bool {
		return consumerGroupList[i] < consumerGroupList[j]
	})

	var consumerGroups []string

	for consumerGroupID := range consumerGroupsMap {
		consumerGroups = append(consumerGroups, consumerGroupID)
	}

	_, err = c.saramaClient.GetConsumerGroupsForTopic(consumerGroups, topic)
	if err != nil {
		return err
	}

	return nil
}
