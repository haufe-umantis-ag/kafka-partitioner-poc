package com.umantis.poc;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Map;

/**
 * @author David Espinosa.
 */
public class KafkaPartitionerProducer extends KafkaProducer {

    public KafkaPartitionerProducer(final Map configs) {
        super(configs);
    }

    public void send(String topic, String user, String msg) {
        super.send(new ProducerRecord<String, String>(topic, user, msg), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                }
                System.out.println("Sent:" + msg + ", User: " + user + ", Partition: " + metadata.partition());
            }
        });
    }
}
