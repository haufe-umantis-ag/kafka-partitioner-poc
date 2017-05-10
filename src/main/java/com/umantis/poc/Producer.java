package com.umantis.poc;

import com.umantis.poc.model.BaseMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import java.util.Map;

/**
 * @author David Espinosa.
 */
public class Producer {

    public static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, BaseMessage> kafkaTemplate;

    public void send(String topic, BaseMessage message) {
        ListenableFuture<SendResult<String, BaseMessage>> future = kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, BaseMessage>>() {

            @Override
            public void onSuccess(final SendResult<String, BaseMessage> stringStringSendResult) {
                LOGGER.info("sent message= " + message + " with offset= " + stringStringSendResult.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("unable to send message= " + message, throwable);
            }
        });
    }
}
