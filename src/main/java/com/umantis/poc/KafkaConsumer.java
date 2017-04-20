package com.umantis.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

//    @KafkaListener(topics = "${kafka.topic}")
//    @KafkaListener(topics = "espi")
//    @KafkaListener(id = "foo", topics = "espi", group = "group1")
    @KafkaListener(topics = "${kafka.topic}")
    public void receive(String message) {
        LOGGER.info("received message='{}'",message);
        latch.countDown();
    }

    public CountDownLatch latch(){
        return latch;
    }
}
