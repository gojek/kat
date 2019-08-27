# KAT

`Kafka Admin Tool` provides an easy interface to perform admin operations on kafka.

### Admin operations available
- List topics
- Describe topics
- Increase replication factor of existing topics
- Show Config for topics
- Alter Config for topics
- Mirror topic configurations from source to destination cluster

### Installation
1. ```go get -u github.com/gojekfarm/kat```
2. Build the project -- 
```make```

#### Using Homebrew

```
brew tap gojek/stable
brew install kat
```

### Usage
List all topics in a kafka cluster

```./kat topic list --broker-list "broker1,broker2"```

List all topics with a particular replication factor

```./kat topic list --broker-list "broker1,broker2" --replication-factor <r>```

Describe topics

```./kat topic describe --broker-list "broker1,broker2" --topics "topic1,topic2"```

Increase replication factor for topics

```./kat topic increase-replication-factor --broker-list "broker1,broker2" --zookeeper "zookeeper1,zookeeper2" --topics "topic1,topic2" --replication-factor <r> --num-of-brokers <n> --kafka-path </path/to/kafka/binary>```

Show Config for topics

```./kat topic config show --topics "topic1,topic2" --broker-list "broker1,broker2"```

Alter Config for topics

```./kat topic config alter --topics "topic1,topic2" --broker-list "broker1,broker2" --config "retention.ms=500000000"```

Mirror topic configs from source to destination cluster

- Specify topics to mirror.

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --topics topic1,topic2```

- Specify topics to mirror and create the topics if not present on destination cluster. ```--create-topics true``` flag is set.

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --topics topic1,topic2 --create-topics true```

- Mirror all topics and create the topics if not present

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --create-topics true```


Help

```./kat --help```

### Future Scope
- Add support for more admin operations


## License

```
Copyright 2019, GO-JEK Tech (http://gojek.tech)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

Note
- update the latest release version to `https://github.com/gojek/homebrew-tap` 
