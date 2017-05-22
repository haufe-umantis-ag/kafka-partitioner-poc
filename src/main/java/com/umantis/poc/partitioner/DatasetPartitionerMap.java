package com.umantis.poc.partitioner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author David Espinosa.
 */
@Component
public class DatasetPartitionerMap {

    private Map<String, Integer> partitionDatasetMap;

    private DatasetPartitionConsumer consumer;

    @Autowired
    public DatasetPartitionerMap(final DatasetPartitionConsumer consumer) {
        this.consumer = consumer;
        List<DatasetPartitionMessage> datasetPartitionMessages = consumer.retrieveMessages();
        Map<String, Integer> collect = datasetPartitionMessages.stream().collect(Collectors.toMap(DatasetPartitionMessage::getDatasetId, DatasetPartitionMessage::getPartitionId));
        this.partitionDatasetMap = collect;
    }

    public int getPartitionForDatasetId(String datasetId) {
        if (partitionDatasetMap.containsKey(datasetId)) {
            return partitionDatasetMap.get(datasetId);
        }
        return -1;
    }

    public void addPartitionDatasetId(DatasetPartitionMessage datasetPartitionMessage) {
        partitionDatasetMap.put(datasetPartitionMessage.getDatasetId(), datasetPartitionMessage.getPartitionId());
    }
}
