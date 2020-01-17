package base

import (
	"strings"

	"github.com/gojek/kat/pkg/client"
	"github.com/gojek/kat/pkg/model"

	"github.com/gojek/kat/logger"
	"github.com/kevinburke/ssh_config"
	"github.com/mitchellh/go-homedir"
)

type Cmd struct {
	cobraUtil  *CobraUtil
	enableSSH  bool
	brokerAddr string
	topic      *model.Topic
	partition  *model.Partition
}

type Opts func(cmd *Cmd)

func Init(cobraUtil *CobraUtil, opts ...Opts) *Cmd {
	baseCmd := &Cmd{cobraUtil: cobraUtil, enableSSH: false, brokerAddr: "broker-list"}

	for _, opt := range opts {
		opt(baseCmd)
	}

	baseCmd.setTopic()
	return baseCmd
}

func WithSSH() Opts {
	return func(baseCmd *Cmd) {
		baseCmd.enableSSH = true
	}
}

func WithPartition(zookeeper string) Opts {
	return func(baseCmd *Cmd) {
		baseCmd.partition = model.NewPartition(zookeeper)
	}
}

func WithAddr(brokerAddr string) Opts {
	return func(baseCmd *Cmd) {
		baseCmd.brokerAddr = brokerAddr
	}
}

func (b *Cmd) setTopic() {
	addr := strings.Split(b.cobraUtil.GetStringArg(b.brokerAddr), ",")
	var opts []model.TopicOpts
	if b.enableSSH {
		keyFile, err := homedir.Expand(b.cobraUtil.GetStringArg("ssh-key-file-path"))
		if err != nil {
			logger.Fatalf("Error while resolving ssh data directory - %v\n", err)
		}
		opts = append(opts, model.WithSSHClient(ssh_config.Get("*", "User"), b.cobraUtil.GetStringArg("ssh-port"), keyFile))
	}
	topic, err := model.NewTopic(client.NewSaramaClient(addr), opts...)
	if err != nil {
		logger.Fatalf("Err on creating topic client - %v\n", err)
	}

	b.topic = topic
}

func (b *Cmd) GetTopic() *model.Topic {
	return b.topic
}

func (b *Cmd) GetPartition() *model.Partition {
	return b.partition
}
