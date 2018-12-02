1. Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

2. Start Kafka
bin/kafka-server-start.sh config/server.properties

3. Create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic customer-transactions

4. Start console consumer.
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic customer-transactions \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer