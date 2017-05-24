package com.umantis.poc.admin;

import java.util.List;

/**
 * Kafka utilities
 *
 * @author David Espinosa.
 * @author Gergely Szakács
 */
public interface KafkaAdminUtils {

    String KAFKA_RETENTION_TIME_PROPERTY = "retention.ms";

    /**
     * Marks a topic for deletion
     *
     * @param topic
     */
    void markTopicForDeletion(String topic);

    /**
     * Retrieves retention.ms property if set, else returns -1
     *
     * @param topic
     * @return
     */
    long getTopicRetentionTime(String topic);

    /**
     * Sets retention.ms topic property
     *
     * @param topic
     * @param retentionTimeInMs
     */
    void setTopicRetentionTime(String topic, long retentionTimeInMs);

    /**
     * Creates a topic
     * auto.create.topics.enable must be set to True (enabled by default)
     *
     * @param topic
     * @param retentionTimeInMs
     */
    void createTopic(String topic, long retentionTimeInMs);

    /**
     * True if topic exists
     *
     * @param topic
     * @return
     */
    boolean topicExists(String topic);

    /**
     * Returns number of partitions for given topic
     *
     * @param topic
     * @return
     */
    int getTopicPartitionsSize(String topic);

    /**
     * Extends topic by an adding additional partitions with given replicationFactor
     *
     * @param topic
     * @param partitions
     * @param replicationFactor
     */
    void extendPartition(final String topic, int partitions, int replicationFactor);

    /**
     * Extends topic by an adding a single additional partition with given replicationFactor
     *
     * @param topic
     * @param replicationFactor
     */
    void addPartition(final String topic, int replicationFactor);

    /**
     * Lists all topics
     *
     * @return
     */
    List<String> listTopics();
}
