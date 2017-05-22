package com.umantis.poc.partitioner;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author David Espinosa.
 */
@Component
public class DataPartitionerService {

    private DatasetPartitionerMap datasetPartitionerMap;

    private KafkaAdminUtils kafkaAdminUtils;

    private DatasetPartitionProducer datasetPartitionProducer;

    @Autowired
    public DataPartitionerService(final DatasetPartitionerMap datasetPartitionerMap, final KafkaAdminUtils kafkaAdminUtils, final DatasetPartitionProducer datasetPartitionProducer) {
        this.datasetPartitionerMap = datasetPartitionerMap;
        this.kafkaAdminUtils = kafkaAdminUtils;
        this.datasetPartitionProducer = datasetPartitionProducer;
    }

    // datasetId retrieved from request scope?
    public int getPartitionForDatasetId(String topic, String datasetId) {
        int partitionForDatasetId = datasetPartitionerMap.getPartitionForDatasetId(datasetId);
        if (partitionForDatasetId ==-1) {
            partitionForDatasetId = createtPartitionrDatasetConfiguration(topic, datasetId);
        }
        return partitionForDatasetId;
    }

    private int createtPartitionrDatasetConfiguration(final String topic, final String datasetId) {
        int partitionForDatasetId;
        kafkaAdminUtils.addPartition(topic, 1);
        int topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(topic);
        int datasetPartition = topicPartitionsSize-1;
        DatasetPartitionMessage datasetPartitionerMessage = createDatasetPartitionerMessage(topic, datasetId, datasetPartition);
        datasetPartitionProducer.send(datasetPartitionerMessage);
        datasetPartitionerMap.addPartitionDatasetId(datasetPartitionerMessage);
        partitionForDatasetId = datasetPartitionerMessage.getPartitionId();
        return partitionForDatasetId;
    }

    private DatasetPartitionMessage createDatasetPartitionerMessage(String topic, String datasetId, int partitionId) {
        DatasetPartitionMessage message = DatasetPartitionMessage.getBuilder()
                .topic(topic)
                .message("Partition assignment for datasetId " + datasetId + " and TOPIC " + topic)
                .origin("DatasetPartitionProducer")
                .datasetId(datasetId)
                .setPartitionId(partitionId)
                .build();
        return message;
    }

}
