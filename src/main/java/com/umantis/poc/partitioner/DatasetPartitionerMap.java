package com.umantis.poc.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This maps loads all existing relations between dataSets and partitions.
 *
 * @author David Espinosa.
 * @author Gergely Szakács
 */
@Component
public class DataSetPartitionerMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSetPartitionerMap.class);

    private Map<String, Integer> partitionDataSetMap;

    private DataSetPartitionConsumer consumer;

    @Autowired
    public DataSetPartitionerMap(final DataSetPartitionConsumer consumer) {
        this.consumer = consumer;
        loadDataSetPartitionMessages();
    }

    /**
     * If a partition assignation exists for given dataSet, the is returned.
     * Else -1 is returned
     *
     * @param datasetId
     * @return
     */
    public int getPartitionForDataSetId(String datasetId) {
        if (partitionDataSetMap.containsKey(datasetId)) {
            return partitionDataSetMap.get(datasetId);
        }
        return -1;
    }

    /**
     * Adds a relation between a dataSet and a partition
     *
     * @param dataSetPartitionMessage
     */
    public void addPartitionDataSetId(DataSetPartitionMessage dataSetPartitionMessage) {
        partitionDataSetMap.put(dataSetPartitionMessage.getDatasetId(), dataSetPartitionMessage.getPartitionId());
    }

    private void loadDataSetPartitionMessages() {
        List<DataSetPartitionMessage> dataSetPartitionMessages = consumer.retrieveMessages();
        Map<String, Integer> collect = dataSetPartitionMessages.stream().collect(Collectors.toMap(DataSetPartitionMessage::getDatasetId, DataSetPartitionMessage::getPartitionId));
        if (collect.size() > 0) {
            LOGGER.info("Found '{}' dataSet partition configuration messages.", collect.size());
        } else {
            LOGGER.info("No dataSet partition configuration found.");
        }
        this.partitionDataSetMap = collect;
    }
}
