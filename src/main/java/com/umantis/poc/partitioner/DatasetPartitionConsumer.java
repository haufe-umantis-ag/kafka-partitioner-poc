package com.umantis.poc.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class reads all messages existing into the configuration partition to retrieves all configuration messages that define the
 * relations between a dataSet and a concrete partition.
 *
 * @author David Espinosa.
 */
public class DataSetPartitionConsumer extends KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSetPartitionConsumer.class);

    @Value("${partition.topic}")
    protected String partitionerTopic;

    public DataSetPartitionConsumer(final Map configs, final Deserializer keyDeserializer, final Deserializer valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }

    /**
     * Retrieves all messages exisiting in the partitioner topic
     *
     * @return
     */
    public List<DataSetPartitionMessage> retrieveMessages() {
        subscribe(Arrays.asList(partitionerTopic));
        ConsumerRecords<String, DataSetPartitionMessage> records = poll(10000L);
        //        for (ConsumerRecord<String, DataSetPartitionerMessage> record : records) {
        //            System.out.println("Found message  " + record.key() + record.value());
        //        }
        unsubscribe();
        List<DataSetPartitionMessage> collect = StreamSupport.stream(records.spliterator(), false)
                .map(p -> p.value()).collect(Collectors.toList());
        LOGGER.info("Recovered '{}' messages from topic: '{}'", collect.size(), partitionerTopic);
        return collect;
    }
}
