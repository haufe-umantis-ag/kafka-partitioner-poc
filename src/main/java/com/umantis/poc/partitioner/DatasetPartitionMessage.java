package com.umantis.poc.partitioner;

import com.umantis.poc.model.BaseMessage;

/**
 * @author David Espinosa.
 */
public class DatasetPartitionMessage extends BaseMessage {

    private int partitionId;

    public DatasetPartitionMessage() {
        super();
    }

    public DatasetPartitionMessage(final int partitionId) {
        this.partitionId = partitionId;
    }

    public DatasetPartitionMessage(final String topic, final String message, final String origin, final String customerId, final int partitionId) {
        super(topic, message, origin, customerId);
        this.partitionId = partitionId;
    }

    public DatasetPartitionMessage(final long time, final String topic, final String message, final String origin, final String customerId, final int partitionId) {
        super(time, topic, message, origin, customerId);
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(final int partitionId) {
        this.partitionId = partitionId;
    }


    public static DatasetPartitionMessageBuilder getBuilder() {
        return new DatasetPartitionMessageBuilder();
    }

    public static class DatasetPartitionMessageBuilder {

        private int partitionId;
        private String topic;
        private String message;
        private String origin;
        private String datasetId;

        public DatasetPartitionMessageBuilder setPartitionId(final int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public DatasetPartitionMessageBuilder topic(final String topic) {
            this.topic = topic;
            return this;
        }

        public DatasetPartitionMessageBuilder message(final String message) {
            this.message = message;
            return this;
        }

        public DatasetPartitionMessageBuilder origin(final String origin) {
            this.origin = origin;
            return this;
        }

        public DatasetPartitionMessageBuilder datasetId(final String datasetId) {
            this.datasetId = datasetId;
            return this;
        }

        public DatasetPartitionMessage build() {
            return new DatasetPartitionMessage(topic, message, origin, datasetId, partitionId);
        }
    }
}
