package com.umantis.poc.partitioner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author David Espinosa.
 */
@Component
public class DatasetPartitionerMap {

    private HashMap<String, DatasetPartitionMessage> partitionDatasetMap;

    private DatasetPartitionConsumer consumer;

    @Autowired
    public DatasetPartitionerMap(final DatasetPartitionConsumer consumer) {
        this.consumer = consumer;
        List<DatasetPartitionMessage> datasetPartitionMessages = consumer.retrieveMessages();
        datasetPartitionMessages.stream().collect(Collectors.toMap(DatasetPartitionMessage::getDatasetId, DatasetPartitionMessage::getPartitionId));
    }

    public int getPartitionForDatasetId(String datasetId) {
        if (partitionDatasetMap.containsKey(datasetId)) {
            return partitionDatasetMap.get(datasetId).getPartitionId();
        }
        return -1;
    }
}
