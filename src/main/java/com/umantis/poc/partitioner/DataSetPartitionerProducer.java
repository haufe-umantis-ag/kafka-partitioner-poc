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
 * Common producer for sending DataSerPartitionerMessages to the related topic.
 *
 * @author David Espinosa.
 * @author Gergely Szakács
 */
public class DataSetPartitionerProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(DataSetPartitionerProducer.class);

    private KafkaTemplate<String, DataSetPartitionMessage> kafkaTemplate;

    @Value("${partition.topic}")
    private String partitionerTopic;

    @Autowired
    public DataSetPartitionerProducer(KafkaTemplate<String, DataSetPartitionMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(DataSetPartitionMessage message) {
        ListenableFuture<SendResult<String, DataSetPartitionMessage>> future = kafkaTemplate.send(partitionerTopic, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, DataSetPartitionMessage>>() {

            @Override
            public void onSuccess(final SendResult<String, DataSetPartitionMessage> sendResult) {
                LOGGER.info("sent message= " + message + " with offset= " + sendResult.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("unable to send message= " + message, throwable);
            }
        });
    }
}
