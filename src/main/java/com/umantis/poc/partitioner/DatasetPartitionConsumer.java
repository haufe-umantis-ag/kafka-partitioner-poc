package com.umantis.poc.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author David Espinosa.
 */
public class DatasetPartitionConsumer extends KafkaConsumer {

    @Value("${partition.topic}")
    protected String partitionerTopic;

    public DatasetPartitionConsumer(final Map configs, final Deserializer keyDeserializer, final Deserializer valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }

    public List<DatasetPartitionMessage> retrieveMessages() {
        subscribe(Arrays.asList(partitionerTopic));
        ConsumerRecords<String, DatasetPartitionMessage> records = poll(1000L);
        for (ConsumerRecord<String, DatasetPartitionMessage> record : records) {
            System.out.println("Found message  " + record.key() + record.value());
        }
        unsubscribe();
//        close();
        List<DatasetPartitionMessage> collect = StreamSupport.stream(records.spliterator(), false)
                .map(p -> p.value()).collect(Collectors.toList());
        return collect;
    }
}
