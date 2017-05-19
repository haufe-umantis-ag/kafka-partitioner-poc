package com.umantis.poc.model;

public class BaseMessage {

  protected long time;
  protected String topic;
  protected String message;
  protected String origin;
  protected String datasetId;

  public BaseMessage() {
    super();
  }

  public BaseMessage(final String topic, final String message, final String origin, final String datasetId) {
    super();
    this.topic = topic;
    this.message = message;
    this.origin = origin;
    this.datasetId = datasetId;
    this.time = System.currentTimeMillis();
  }

  public BaseMessage(final long time, final String topic, final String message, final String origin, final String datasetId) {
    this.time = time;
    this.topic = topic;
    this.message = message;
    this.origin = origin;
    this.datasetId = datasetId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getMessage() {
    return message;
  }


  public void setMessage(String message) {
    this.message = message;
  }

  public String getOrigin() {
    return origin;
  }


  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public long getTime() {
    return time;
  }

  public void setTime(final long time) {
    this.time = time;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(final String datasetId) {
    this.datasetId = datasetId;
  }

  @Override
  public String toString() {
    return "BaseMessage{" +
            "time=" + time +
            ", topic='" + topic + '\'' +
            ", message='" + message + '\'' +
            ", origin='" + origin + '\'' +
            '}';
  }

  public static BaseMessageBuilder builder() {
    return new BaseMessageBuilder();
  }

  public static class BaseMessageBuilder {

    private String message;
    private String origin;
    private String datasetId;
    private String topic;

    BaseMessageBuilder() {
    }

    public BaseMessage.BaseMessageBuilder message(final String message) {
      this.message = message;
      return this;
    }

    public BaseMessage.BaseMessageBuilder origin(final String origin) {
      this.origin = origin;
      return this;
    }

    public BaseMessage.BaseMessageBuilder customerId(final String datasetId) {
      this.datasetId = datasetId;
      return this;
    }

    public BaseMessage.BaseMessageBuilder topic(final String topic) {
      this.topic = topic;
      return this;
    }

    public BaseMessage build() {
      return new BaseMessage(origin, message, topic, datasetId);
    }
  }
}
