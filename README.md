# KAT

`Kafka Admin Tool` provides an easy interface to perform admin operations on kafka.

### Admin operations available
- List topics
- Describe topics
- Increase replication factor of existing topics
- Reassign partitions for topics
- Show Config for topics
- Alter Config for topics
- Mirror topic configurations from source to destination cluster

### Installation
1. ```go get -u github.com/gojekfarm/kat```
2. Build the project -- 
```make```
3. Multiple kafka cluster can be created in local environment using docker-compose
```docker-compose up -d```

#### Using Homebrew
    
```
brew tap gojek/stable
brew install kat
```

### Usage
*List all topics in a kafka cluster*

```./kat topic list --broker-list "broker1,broker2"```

*List all topics with a particular replication factor*

```./kat topic list --broker-list "broker1,broker2" --replication-factor <r>```

*List all topics with last write time before given time (unused/stale topics)*

```./kat topic list --broker-list="broker" --last-write=<epochtime> --data-dir=<kafka logs directory>```

*Describe topics*

```./kat topic describe --broker-list "broker1,broker2" --topics "topic1,topic2"```

*Delete topics*

- All the topics matches the whitelist regex will get deleted
    ```./kat topic list --broker-list="broker" --topic-whitelist=test```

- All the topics does not match the blacklist regex will get deleted

    ```./kat topic list --broker-list="broker" --topic-blacklist=test```

- All the topics which last received data before the given time and matches whitelist regex will get deleted (unused/staled)

    ```./kat topic list --broker-list="broker" --last-write=<epochtime> --data-dir=<kafka logs directory>  --topic-whitelist=test```

- All the topics which last received data before the given time and matches the blacklist regex will get deleted (unused/staled)

    ```./kat topic list --broker-list="broker" --last-write=<epochtime> --data-dir=<kafka logs directory>  --topic-blacklist=test```

*Increase replication factor for topics*

```./kat topic increase-replication-factor --broker-list "broker1,broker2" --zookeeper "zookeeper1,zookeeper2" --topics "topic1|topic2.*" --replication-factor <r> --num-of-brokers <n> --batch <b> --timeout-per-batch <t> --poll-interval <p> --throttle <t>```

*Reassign Partitions for topics*

```./kat topic reassign-partitions --broker-list "broker1,broker2" --zookeeper "zookeeper1,zookeeper2" --topics "topic1|topic2.*" --broker-ids <i> --batch <b> --timeout-per-batch <t> --poll-interval <p> --throttle <t>```

*Show Config for topics*

```./kat topic config show --topics "topic1,topic2" --broker-list "broker1,broker2"```

*Alter Config for topics*

```./kat topic config alter --topics "topic1,topic2" --broker-list "broker1,broker2" --config "retention.ms=500000000"```

*Mirror topic configs from source to destination cluster*

- Specify topics to mirror.

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --topics topic1,topic2```

- Specify topics to mirror and create the topics if not present on destination cluster. ```--create-topics true``` flag is set.

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --topics topic1,topic2 --create-topics```

- Mirror all topics and create the topics if not present

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --create-topics```

-To increase the partition count

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --increase-partitions```

-To dry run and see the changes before applying the mirror command, 

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --increase-partitions --dry-run```

-To exclude mirroring certain configs, 

```./kat mirror --source-broker-ips broker1,broker2  --destination-broker-ips broker1,broker2 --exclude-configs "follower.replication.throttled.replicas,leader.replication.throttled.replicas"
```

*Help*

```./kat --help```

```./kat <command> --help``` will list all the configs, their meanings and defaults per given command

### Partition Reassignment
```./kat topic reassign-partitions --broker-list "broker1,broker2" --zookeeper "zookeeper1,zookeeper2" --topics "topic1|topic2.*" --broker-ids <i> --batch <b> --timeout-per-batch <t> --poll-interval <p> --throttle <t>```

This command reassigns partitions for the given topics among the passed broker ids. This can be very time consuming, if run during the busy hours of the kafka cluster, and it is not straight forward to kill the process and rollback.

Hence the command accepts a batch argument with a default of 1, which will split the topics passed into batches of the given number and run the reassignment per batch. Only if a batch completes successfully within the given timeout - `timeout-per-batch`, will the next batch start.
The status of reassignment is checked every `poll-interval` seconds until the `timeout-per-batch` exceeds. In the event of failure of any batch, the command terminates. Only the topics in the failed batch need to be debugged/rolled back.

All the rollback and reassignment json files are stored in /tmp path, one file per batch for each of rollback and reassignment. In case of any failure, running the `kafka-reassign-partitions` by passing the rollback.json for the failed batch will restore the state of those partitions.

### Future Scope
- Add support for more admin operations

## TODO
Refactoring
* [ ] move sarama deps, and building command/running to a baseCmd and compose it in all cmds
* [ ] get rid of topicutil, move functions to topics struct (group behaviour: list, delete, create, update)
* [ ] mirror, all commands should use same flags (can use config file for default value so we can pass less flags while running)
* [ ] inject custom interface with only required functionality instead of saramaCli ()
* [ ] add golangci lint
* [ ] add make file (vet,lint,goimports,test with race)
* [ ] introduce logrus with log levels
* [ ] helper for executing shell commands
* [ ] beautify the UI for show config command

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


- update the latest release version to `https://github.com/gojek/homebrew-tap` 

