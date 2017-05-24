package com.umantis.poc.partitioner;

import com.umantis.poc.model.BaseMessage;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Map;

/**
 * Custom Partitioner
 *
 * @author David Espinosa
 * @author Gergely Szakács
 */
@Deprecated
@Component
public class KafkaUserCustomPartitioner implements Partitioner {

    @Autowired
    private DataSetPartitionerService dataSetPartitionerService;

    public KafkaUserCustomPartitioner() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
            Cluster cluster) {

        BaseMessage baseMessage = (BaseMessage) value;
        // Find the id of current user based on the username
        int userId = dataSetPartitionerService.getPartitionForDataSetId(baseMessage.getTopic(), baseMessage.getDatasetId());
        // If the userId not found, default partition is 0
        return userId;
    }

    @Override
    public void close() {

    }
}
