package com.umantis.poc.partitioner;

/**
 * @author David Espinosa.
 */
public interface DataSetPartitionerService {

    // datasetId retrieved from request scope?

    /**
     * Returns assigned partition for given dataSet if exists.
     * If doesn't, a new one is created.
     *
     * @param topic
     * @param datasetId
     * @return
     */
    int getPartitionForDataSetId(String topic, String datasetId);
}
