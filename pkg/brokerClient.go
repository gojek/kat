package pkg

import (
	"bytes"
	"fmt"
	"golang.org/x/crypto/ssh"
	"strings"
)

type sshCli interface {
	Dial(address string) (*ssh.Client, error)
	Execute(client *ssh.Client, cmd string) (*bytes.Buffer, error)
}

type BrokerClient struct {
	KafkaApiClient
	sshCli
}

func NewBrokerClient(apiClient KafkaApiClient, user, port, keyfile string) (KafkaSshClient, error) {
	sshClient, err := NewSshClient(user, port, keyfile)
	if err != nil {
		return nil, err
	}
	return &BrokerClient{apiClient, sshClient}, nil
}

func (s *BrokerClient) ListTopics(request ListTopicsRequest) ([]string, error) {
	brokers := s.ListBrokers()

	for id, ip := range brokers {
		fmt.Printf("Sshing into broker - %v\n", id)
		client, err := s.sshCli.Dial(strings.Split(ip, ":")[0])
		if err != nil {
			fmt.Printf("Error while dialing ssh session - %v\n", err)
			return nil, err
		}

		data, err := s.sshCli.Execute(client, "ls /data")
		if err != nil {
			fmt.Printf("Error while executing remote command - %v\n", err)
			return nil, err
		}

		fmt.Println("---------------- data --------------")
		fmt.Println(data)
	}

	return nil, nil
}
