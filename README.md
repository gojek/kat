# Kafka-Admin-Tools

This tool provides an easy interface to perform admin operations on kafka.

### Admin operations available
- List topics
- Describe topics
- Increase replication factor of existing topics

### Installation
1. Clone the repository --
```git clone git@source.golabs.io:hermes/kafka-admin-tools.git```
2. Build the project -- 
```make```

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

Help

```./kat --help```

### Future Scope
- Add support for more admin operations
