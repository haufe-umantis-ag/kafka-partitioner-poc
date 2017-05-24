package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.partitioner.DataSetPartitionConsumer;
import com.umantis.poc.partitioner.DataSetPartitionMessage;
import com.umantis.poc.partitioner.DataSetPartitionerProducer;
import com.umantis.poc.users.UserService;
import com.umantis.poc.users.UserServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;

/**
 * @author David Espinosa.
 * @author Gergely Szakács
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class InitialLoadTest {

    @Autowired
    public DataSetPartitionerProducer partitionerProducer;

    @Autowired
    public DataSetPartitionConsumer partitionerConsumer;

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

    private UserService userService = new UserServiceImpl();
    private static String TOPIC;

    @Value("${partition.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Before
    public void checkTopic() {
        if (kafkaAdminUtils.topicExists(TOPIC)) {
            TOPIC += System.currentTimeMillis();
            ReflectionTestUtils.setField(partitionerProducer, "partitionerTopic", TOPIC);
            ReflectionTestUtils.setField(partitionerConsumer, "partitionerTopic", TOPIC);
        }
        kafkaAdminUtils.createTopic(TOPIC, -1);
    }

    @Test()
    public void given_emptyDatasetPartitionTopic_when_newDatasetPartitionMessagesAreCreated_then_canBeRetrievedFromTopic() throws Exception {
        flushData();
        List<DataSetPartitionMessage> datasetPartitionMessages = partitionerConsumer.retrieveMessages();
        Assertions.assertThat(userService.findAllUsers().size() == datasetPartitionMessages.size());
    }

    private void flushData() {
        int partitionCounter = 1;
        for (String user : userService.findAllUsers()) {
            DataSetPartitionMessage message = DataSetPartitionMessage.getBuilder()
                    .topic(TOPIC)
                    .message("Partition assignment for user " + user + " and TOPIC " + TOPIC)
                    .origin("DatasetPartitionerTest")
                    .datasetId(user)
                    .setPartitionId(partitionCounter)
                    .build();
            partitionerProducer.send(message);
            partitionCounter++;
        }
    }

    @After
    public void tearDown() {
        kafkaAdminUtils.markTopicForDeletion(TOPIC);
    }
}
