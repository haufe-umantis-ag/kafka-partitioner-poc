package com.umantis.poc.admin;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.collection.JavaConversions;
import java.util.List;
import java.util.Properties;

/**
 * @author David Espinosa.
 */
@Service
public class KafkaAdminUtilsImpl implements KafkaAdminUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminUtilsImpl.class);

    private ZkConnection zkConnection;
    private ZkClient zkClient;
    private ZkUtils zkUtils;

    @Autowired
    public KafkaAdminUtilsImpl(final ZkConnection zkConnection, final ZkClient zkClient) {
        this.zkConnection = zkConnection;
        this.zkClient = zkClient;
        zkUtils = new ZkUtils(zkClient, zkConnection, false);
    }

    @Override
    public void markTopicForDeletion(String topic) {
        if (topicExists(topic)) {
            AdminUtils.deleteTopic(zkUtils, topic);
            LOGGER.info("Topic " + topic + " marked for deletion. This will have no effect if property delete.topic.enable is disabled.");
        }
    }

    @Override
    public long getTopicRetentionTime(String topic) {
        long topicRetentionTime = -1;
        Properties topicProperties = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        if (topicProperties.containsKey(KAFKA_RETENTION_TIME_PROPERTY)) {
            topicRetentionTime = Long.valueOf((String) topicProperties.get(KAFKA_RETENTION_TIME_PROPERTY));
        }
        return topicRetentionTime;
    }

    @Override
    public void setTopicRetentionTime(String topic, long retentionTimeInMs) {
        Properties properties = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        properties.put(KAFKA_RETENTION_TIME_PROPERTY, String.valueOf(retentionTimeInMs));
        AdminUtils.changeTopicConfig(zkUtils, topic, properties);
        LOGGER.info("Changed retention time to: " + retentionTimeInMs + " for topic " + topic);
    }

    @Override
    public void createTopic(String topic, long retentionTimeInMs) {
        Properties properties = new Properties();
        properties.put(KAFKA_RETENTION_TIME_PROPERTY, String.valueOf(retentionTimeInMs));
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        LOGGER.info("Created topic: " + retentionTimeInMs + " for topic " + topic);
    }

    @Override
    public boolean topicExists(final String topic) {
        return AdminUtils.topicExists(zkUtils, topic);
    }

    @Override
    public List<String> listTopics() {
        return JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
    }

    public static List<ACL> getDefaultACLs() {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
}
