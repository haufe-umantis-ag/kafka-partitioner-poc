package com.umantis.poc;

import com.umantis.poc.model.BaseMessage;
import com.umantis.poc.partitioner.DataSetPartitionServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author David Espinosa.
 */
public class PartitionerProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(PartitionerProducer.class);

    @Autowired
    private KafkaTemplate<String, BaseMessage> kafkaTemplate;

    @Autowired
    private DataSetPartitionServiceImpl dataSetPartitionerService;

    public void send(String topic, BaseMessage message) {
        //partition assigned to dataSet is provided by the DataPartitionerService, if no existing partition is assigned a new one is created.
        int partition = dataSetPartitionerService.getPartitionForDataSetId(topic, message.getDatasetId());
        ListenableFuture<SendResult<String, BaseMessage>> future = kafkaTemplate.send(topic, partition, message.getDatasetId(), message);
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
