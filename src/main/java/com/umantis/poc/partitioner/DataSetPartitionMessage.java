package com.umantis.poc.partitioner;

import com.umantis.poc.model.BaseMessage;

/**
 * @author David Espinosa.
 */
public class DataSetPartitionMessage extends BaseMessage {

    private int partitionId;

    public DataSetPartitionMessage() {
        super();
    }

    public DataSetPartitionMessage(final int partitionId) {
        this.partitionId = partitionId;
    }

    public DataSetPartitionMessage(final String topic, final String message, final String origin, final String customerId, final int partitionId) {
        super(topic, message, origin, customerId);
        this.partitionId = partitionId;
    }

    public DataSetPartitionMessage(final long time, final String topic, final String message, final String origin, final String customerId, final int partitionId) {
        super(time, topic, message, origin, customerId);
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(final int partitionId) {
        this.partitionId = partitionId;
    }


    public static DataSetPartitionMessageBuilder getBuilder() {
        return new DataSetPartitionMessageBuilder();
    }

    public static class DataSetPartitionMessageBuilder {

        private int partitionId;
        private String topic;
        private String message;
        private String origin;
        private String datasetId;

        public DataSetPartitionMessageBuilder setPartitionId(final int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public DataSetPartitionMessageBuilder topic(final String topic) {
            this.topic = topic;
            return this;
        }

        public DataSetPartitionMessageBuilder message(final String message) {
            this.message = message;
            return this;
        }

        public DataSetPartitionMessageBuilder origin(final String origin) {
            this.origin = origin;
            return this;
        }

        public DataSetPartitionMessageBuilder datasetId(final String datasetId) {
            this.datasetId = datasetId;
            return this;
        }

        public DataSetPartitionMessage build() {
            return new DataSetPartitionMessage(topic, message, origin, datasetId, partitionId);
        }
    }
}
