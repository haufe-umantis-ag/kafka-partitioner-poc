package com.umantis.poc.partitioner;

import com.umantis.poc.model.BaseMessage;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class KafkaUserCustomPatitioner implements Partitioner {

    private IUserService userService;

    @Autowired
    private DataSetPartitionerMap datasetPartitionerMap;

    public KafkaUserCustomPatitioner() {
        userService = new UserServiceImpl();
    }

//    @Autowired
//    public KafkaUserCustomPatitioner(DatasetPartitionerMap datasetPartitionerMap) {
//        userService = new UserServiceImpl();
//        this.datasetPartitionerMap = datasetPartitionerMap;
//    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
            Cluster cluster) {

        BaseMessage baseMessage = (BaseMessage) value;
        int partition = 0;
        // Find the id of current user based on the username
        Integer userId = userService.findUserId(baseMessage.getDatasetId());
        // If the userId not found, default partition is 0
        if (userId != null) {
            partition = userId;
        }
        return partition;
    }


    @Override
    public void close() {

    }
}
