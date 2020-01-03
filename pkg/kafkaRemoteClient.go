package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/ssh"
	"strings"
	"time"
)

type sshCli interface {
	Dial(address string) (*ssh.Client, error)
	Execute(client *ssh.Client, cmd string) (*bytes.Buffer, error)
}

type KafkaRemoteClient struct {
	KafkaApiClient
	sshCli
}

func NewKafkaSshCli(apiClient KafkaApiClient, user, port, keyfile string) (KafkaSshClient, error) {
	sshClient, err := NewSshClient(user, port, keyfile)
	if err != nil {
		return nil, err
	}
	return &KafkaRemoteClient{apiClient, sshClient}, nil
}

func (s *KafkaRemoteClient) ListTopics(request ListTopicsRequest) ([]string, error) {
	brokers := s.ListBrokers()
	dateTime := time.Unix(request.LastWritten, 0)

	fmt.Printf("------------------%v\n", dateTime.UTC().Format(time.UnixDate))

	for id := 1; id <= len(brokers); id++ {
		fmt.Printf("Sshing into broker - %v\n", brokers[id])
		client, err := s.sshCli.Dial(strings.Split(brokers[id], ":")[0])
		if err != nil {
			fmt.Printf("Error while dialing ssh session - %v\n", err)
			return nil, err
		}

		data, err := s.sshCli.Execute(client, fmt.Sprintf("find /data/kafka-logs -maxdepth 1 -not -path \"*/\\.*\" -not -newermt \"%s\"  | xargs -I{} echo {} | rev | cut -d / -f1 | rev | xargs -I{} echo {} | rev | cut -d - -f2-  | rev | sort | uniq -c | awk '{ print $2 \" \" $1}'", dateTime.UTC().Format(time.UnixDate)))
		if err != nil {
			fmt.Printf("Error while executing remote command - %v\n", err)
			return nil, err
		}

		fmt.Println("---------------- data --------------")
		fmt.Println(data.String())

		res := make(map[string]string)
		err = json.Unmarshal(data.Bytes(), &res)

		if err != nil {
			fmt.Printf("%v\n", err)

		}

		fmt.Println("---------------- res --------------")
		fmt.Println(res)

	}

	return nil, nil
}
