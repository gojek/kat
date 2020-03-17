# KAT
[![Build Status](https://circleci.com/gh/gojek/kat.svg?branch=master)](https://circleci.com/gh/gojek/kat)

Kafka Admin Tool provides an interface to perform many admin operations on kafka in a straight-forward manner.

## Installation
### Using Homebrew    
```
brew tap gojek/stable
brew install kat
```

### Others
```
go install github.com/gojek/kat
```

### Local Dev/Testing
* Clone the repo
* Run ```make all``` to run lint checks, unit tests and build the project
* Manual testing: Running ```docker-compose up -d``` will create 2 local kafka clusters. Commands can be run against these clusters for testing

## Admin operations available
- [List Topics](#list-topics)
- [Describe Topics](#describe-topics)
- [Delete Topics](#delete-topics)
- [List Consumer Groups for a topic](#list-consumer-groups-for-a-topic)
- [Increase Replication Factor](#increase-replication-factor)
- [Reassign Partitions](#reassign-partitions)
- [Show Topic Configs](#show-topic-configs)
- [Alter Topic Configs](#alter-topic-configs)
- [Mirror Topic Configs from Source to Destination Cluster](#mirror-topic-configs-from-source-to-destination-cluster)

## Command Usage
### Help
* Display the various args accepted by each command and the corresponding defaults
```
kat --help
kat <cmd> --help
```

### List Topics
* List all the topics in a cluster
```
kat topic list --broker-list <"broker1:9092,broker2:9092">
```

* List all topics with a particular replication factor
```
kat topic list --broker-list <"broker1:9092,broker2:9092"> --replication-factor <replication factor>
```

* List all topics with last write time before given time (unused/stale topics)
```
kat topic list --broker-list <"broker1:9092,broker2:9092"> --last-write=<epoch time> --data-dir=<kafka logs directory>
```

Topic throughput metrics or last modified time is not available in topic metadata response from kafka. Hence, this tool has a custom implementation of ssh'ing into all the brokers and filtering through the kafka logs directory to find the topics that were not written after the given time. 

### Describe Topics
* Describe metadata for topics
```
kat topic describe --broker-list <"broker1:9092,broker2:9092"> --topics <"topic1,topic2">
```

### Delete Topics

* Delete the topics that match the given topic-whitelist regex
```
kat topic delete --broker-list <"broker1:9092,broker2:9092"> --topic-whitelist=<*test*>
```

* Delete the topics that do not match the given topic-blacklist regex
```
kat topic delete --broker-list <"broker1:9092,broker2:9092"> --topic-blacklist=<*test*>
```

* Delete the topics that are not modified since the last-write epoch time and match the topic-whitelist regex
```
kat topic delete --broker-list <"broker1:9092,broker2:9092"> --last-write=<epoch time> --data-dir=<kafka logs directory>  --topic-whitelist=<*test*>
```

* Delete the topics that are not modified since the last-write epoch time and do not match the topic-blacklist regex
```
kat topic delete --broker-list <"broker1:9092,broker2:9092"> --last-write=<epoch time> --data-dir=<kafka logs directory>  --topic-blacklist=<*test*>
```

### List Consumer Groups for a Topic
* Lists all the consumer groups that are subscribed to a given topic
```
kat consumergroup list -b <"broker1:9092,broker2:9092"> -t <topic-name>
```

### Increase Replication Factor
* Increase the replication factor of topics that match given regex
```
kat topic increase-replication-factor --broker-list <"broker1:9092,broker2:9092"> --zookeeper <"zookeeper1,zookeeper2"> --topics <"topic1|topic2.*"> --replication-factor <r> --num-of-brokers <n> --batch <b> --timeout-per-batch <t> --poll-interval <p> --throttle <t>
```

[Details](#increase-replication-factor-and-partition-reassignment-details)


### Reassign Partitions
* Reassign partitions for topics that match given regex
```
kat topic reassign-partitions --broker-list <"broker1:9092,broker2:9092"> --zookeeper <"zookeeper1,zookeeper2"> --topics <"topic1|topic2.*"> --broker-ids <i,j,k> --batch <b> --timeout-per-batch <t> --poll-interval <p> --throttle <t>
```

[Details](#increase-replication-factor-and-partition-reassignment-details)

### Show Topic Configs
* Show config for topics
```
kat topic config show --topics <"topic1,topic2"> --broker-list <"broker1:9092,broker2:9092">
```

### Alter Topic Configs
* Alter config for topics
```
kat topic config alter --topics <"topic1,topic2"> --broker-list <"broker1:9092,broker2:9092"> --config <"retention.ms=500000000,segment.bytes=1000000000">
```

### Mirror Topic Configs from Source to Destination Cluster
* Mirror all configs for topics present in both source and destination cluster
```
kat mirror --source-broker-ips=<"broker1:9092,broker2:9092"> --destination-broker-ips=<"broker3,broker4">
```

* Mirror configs for topics present in both source and destination cluster, with some configs as exception
```
kat mirror --source-broker-ips=<"broker1:9092,broker2:9092"> --destination-broker-ips=<"broker3,broker4"> --exclude-configs=<"retention.ms,segment.bytes">
```

* Mirror configs for topics present in source cluster, but not in destination cluster
```
kat mirror --source-broker-ips=<"broker1:9092,broker2:9092"> --destination-broker-ips=<"broker3,broker4"> --exclude-configs=<"retention.ms,segment.bytes"> --create-topics
```

* Mirror configs for topics, with increase in partition count if there is a difference
```
kat mirror --source-broker-ips=<"broker1:9092,broker2:9092"> --destination-broker-ips=<"broker3,broker4"> --exclude-configs=<"retention.ms,segment.bytes"> --create-topics --increase-partitions
```

* Preview changes that will be applied on the destination cluster after mirroring
```
kat mirror --source-broker-ips=<"broker1:9092,broker2:9092"> --destination-broker-ips=<"broker3,broker4"> --exclude-configs=<"retention.ms,segment.bytes"> --create-topics --increase-partitions --dry-run
```

#### Increase Replication Factor and Partition Reassignment Details
[Increasing Replication Factor](https://docs.confluent.io/current/kafka/post-deployment.html#increasing-replication-factor) and [Partition Reassignment](https://www.ibm.com/support/knowledgecenter/sv/SSCVHB_1.2.0/admin/tnpi_reassign_partitions.html) are not one step processes. On a high level, the following steps need to be executed:

1. Generating the reassignment.json file
2. Executing `kafka-reassign-partitions` command
3. Verifying the status of reassignment

This tool has automation around all these steps:
1. Topics are split into batches of the number passed in `batch` arg.
2. Reassignment json file is created for each batch. 
    * For increasing replication factor, this file is created using custom round-robin mechanism, that assigns leaders and ISRs per partition.
    * For partition reassignment, this is created using `--generate` flag provided by kafka cli tool.
3. `kafka-reassign-partitions` command is executed for each batch. 
4. Status is polled for every `poll-interval` until the `timeout-per-batch` is reached. If the timeout breaches, the command exits. Once replication factor for all partitions in the batch are increased, then next batch is processed.
5. The reassignment.json and rollback.json files for all the batches are stored in /tmp directory. In case of any failure, running the `kafka-reassign-partitions` by passing the rollback.json of the failed batch will restore the state of those partitions.


## Future Scope
- Add support for more admin operations
- Beautify the response of list and show config commands. Add custom features to ui pkg
- Fetch values from a kat config file instead of passing everything as cmd args

## Contributing
* Raise an issue to clarify scope/questions, followed by PR
* Follow go [guidelines](https://golang.org/doc/effective_go.html) for development
* Ensure `make` succeeds

Thanks for all the [Contributors](https://github.com/gojek/kat/graphs/contributors).

## License
Licensed under the [Apache License](./LICENSE), Version 2.0