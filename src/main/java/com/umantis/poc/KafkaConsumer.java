package com.umantis.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private CountDownLatch latch = new CountDownLatch(5);

//    @KafkaListener(topics = "${kafka.topic}")
//    @KafkaListener(topics = "espi")
//    @KafkaListener(id = "foo", topics = "espi", group = "group1")

//    @KafkaListener(topics = "${kafka.topic}")
//    public void receive(String message, ConsumerRecord partition) {
//        LOGGER.info("received message="+message + " and partition="+partition);
//        latch.countDown();
//    }

    @KafkaListener(topics = "${kafka.topic}")
    public void receive(ConsumerRecord consumerRecord) {
        LOGGER.info("received message="+consumerRecord.value()
                            + ", partition="+consumerRecord.partition()
                            + ", key="+consumerRecord.key()
                            + ", topic="+consumerRecord.topic()
                            + ", timestamp="+consumerRecord.timestamp()
        );
        latch.countDown();
    }

//    @KafkaListener(topics = "${kafka.topic}")
//    public void onReceiving(String message, @Header(KafkaHeaders.OFFSET) Integer offset,
//            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
//            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        LOGGER.info("Processing topic = {}, partition = {}, offset = {}, workUnit = {}",
//                 topic, partition, offset, message);
//    }

    public CountDownLatch latch(){
        return latch;
    }
}
