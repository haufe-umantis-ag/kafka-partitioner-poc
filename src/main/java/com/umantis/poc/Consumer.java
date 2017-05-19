package com.umantis.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch latch = new CountDownLatch(5);

    @KafkaListener(topics = "${kafka.topic}")
    public void receive(ConsumerRecord consumerRecord) {
        LOGGER.info("received message=" + consumerRecord.value()
                            + ", partition=" + consumerRecord.partition()
                            + ", key=" + consumerRecord.key()
                            + ", topic=" + consumerRecord.topic()
                            + ", timestamp=" + consumerRecord.timestamp()
        );
        latch.countDown();
    }

    public CountDownLatch latch() {
        return latch;
    }
}
