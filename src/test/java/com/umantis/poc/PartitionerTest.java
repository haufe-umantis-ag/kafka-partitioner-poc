package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
import com.umantis.poc.partitioner.DatasetPartitionMessage;
import com.umantis.poc.partitioner.DatasetPartitionProducer;
import com.umantis.poc.partitioner.DatasetPartitionerMap;
import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.UserServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Basic partitioner test that checks that sent messages are stored in different partitions
 *
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PartitionerTest {

    @Autowired
    public PartitionerProducer producer;

    @Autowired
    public DatasetPartitionerMap datasetPartitionerMap;

    @Autowired
    public Consumer consumer;

    @Autowired
    public DatasetPartitionProducer datasetPartitionProducer;

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

    private static String TOPIC;

    private static String PARTITIONER_TOPIC;

    @Value("${kafka.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Value("${partition.topic}")
    public void setPartitionerTopic(String partitionerTopic) {
        PARTITIONER_TOPIC = partitionerTopic;
    }

    @Before
    public void checkTopics() {
        if (kafkaAdminUtils.topicExists(TOPIC)) {
            TOPIC += System.currentTimeMillis();
        }
        kafkaAdminUtils.createTopic(TOPIC, -1);

        if (kafkaAdminUtils.topicExists(PARTITIONER_TOPIC)) {
            PARTITIONER_TOPIC += System.currentTimeMillis();
            ReflectionTestUtils.setField(datasetPartitionProducer, "partitionerTopic", PARTITIONER_TOPIC);
            ReflectionTestUtils.setField(datasetPartitionerMap, "partitionDatasetMap", new HashMap<String, Integer>());
        }
        kafkaAdminUtils.createTopic(PARTITIONER_TOPIC, -1);
    }

    @Test()
    public void testReceive() throws Exception {
        IUserService iUserService = new UserServiceImpl();
        for (String user : iUserService.findAllUsers()) {

            BaseMessage message = BaseMessage.builder()
                    .topic(TOPIC)
                    .message("Hello " + user)
                    .origin("PartitionerTest")
                    .customerId(user)
                    .build();
            producer.send(TOPIC, message);
        }

        ConsumerRecords<String, BaseMessage> records = getConsumedRecords();
        int initialPartition = 0;
        for (ConsumerRecord<String, BaseMessage> record : records) {
            Assertions.assertThat(record.partition() == initialPartition + 1);
            initialPartition++;
        }
        List<BaseMessage> collect = StreamSupport.stream(records.spliterator(), false)
                .map(p -> p.value()).collect(Collectors.toList());
        Assertions.assertThat(collect.size() == 5);
    }

    private ConsumerRecords<String, BaseMessage> getConsumedRecords() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaConsumer<String, BaseMessage> kafkaConsumer = new KafkaConsumer<String, BaseMessage>(props, new StringDeserializer(), new JsonDeserializer(DatasetPartitionMessage.class));
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<String, BaseMessage> records = kafkaConsumer.poll(1000L);
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        return records;
    }
}
