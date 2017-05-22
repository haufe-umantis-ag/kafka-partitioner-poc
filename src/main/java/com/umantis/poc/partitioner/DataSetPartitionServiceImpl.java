package com.umantis.poc.partitioner;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author David Espinosa.
 */
@Component
public class DataSetPartitionServiceImpl implements DataSetPartitionerService {

    private DataSetPartitionerMap datasetPartitionerMap;

    private KafkaAdminUtils kafkaAdminUtils;

    private DataSetPartitionerProducer datasetPartitionProducer;

    @Autowired
    public DataSetPartitionServiceImpl(final DataSetPartitionerMap datasetPartitionerMap, final KafkaAdminUtils kafkaAdminUtils, final DataSetPartitionerProducer datasetPartitionProducer) {
        this.datasetPartitionerMap = datasetPartitionerMap;
        this.kafkaAdminUtils = kafkaAdminUtils;
        this.datasetPartitionProducer = datasetPartitionProducer;
    }

    // datasetId retrieved from request scope?
    @Override
    public int getPartitionForDataSetId(String topic, String datasetId) {
        int partitionForDatasetId = datasetPartitionerMap.getPartitionForDataSetId(datasetId);
        if (partitionForDatasetId == -1) {
            partitionForDatasetId = createDatasetPartitionerConfigurationEntry(topic, datasetId);
        }
        return partitionForDatasetId;
    }

    private int createDatasetPartitionerConfigurationEntry(final String topic, final String dataSetId) {
        int partitionForDatasetId;
        kafkaAdminUtils.addPartition(topic, 1);
        int topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(topic);
        int dataSetPartition = topicPartitionsSize - 1;
        DataSetPartitionMessage datasetPartitionerMessage = createDataSetPartitionerMessage(topic, dataSetId, dataSetPartition);
        datasetPartitionProducer.send(datasetPartitionerMessage);
        datasetPartitionerMap.addPartitionDataSetId(datasetPartitionerMessage);
        partitionForDatasetId = datasetPartitionerMessage.getPartitionId();
        return partitionForDatasetId;
    }

    private DataSetPartitionMessage createDataSetPartitionerMessage(String topic, String datasetId, int partitionId) {
        DataSetPartitionMessage message = DataSetPartitionMessage.getBuilder()
                .topic(topic)
                .message("Partition assignment for datasetId " + datasetId + " and TOPIC " + topic)
                .origin("DatasetPartitionProducer")
                .datasetId(datasetId)
                .setPartitionId(partitionId)
                .build();
        return message;
    }
}
