package com.umantis.poc.partitioner;

/**
 * @author David Espinosa.
 * @author Gergely Szakács
 */
public interface DataSetPartitionerService {

    /**
     * Returns assigned partition for given dataSet if exists.
     * If doesn't, a new one is created.
     * In a web service dataSetId could be retrieved from request scope.
     *
     * @param topic
     * @param datasetId
     * @return
     */
    int getPartitionForDataSetId(String topic, String datasetId);
}
