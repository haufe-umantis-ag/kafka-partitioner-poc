package com.umantis.poc.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author David Espinosa.
 */
public class DatasetPartitionProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(DatasetPartitionProducer.class);

    private KafkaTemplate<String, DatasetPartitionMessage> kafkaTemplate;

    @Value("${partition.topic}")
    private String partitionerTopic;

    @Autowired
    public DatasetPartitionProducer(KafkaTemplate<String, DatasetPartitionMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(DatasetPartitionMessage message) {
        ListenableFuture<SendResult<String, DatasetPartitionMessage>> future = kafkaTemplate.send(partitionerTopic, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, DatasetPartitionMessage>>() {

            @Override
            public void onSuccess(final SendResult<String, DatasetPartitionMessage> sendResult) {
                LOGGER.info("sent message= " + message + " with offset= " + sendResult.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("unable to send message= " + message, throwable);
            }
        });
    }

}
