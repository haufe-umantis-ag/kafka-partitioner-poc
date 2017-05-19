package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.partitioner.DatasetPartitionConsumer;
import com.umantis.poc.partitioner.DatasetPartitionMessage;
import com.umantis.poc.partitioner.DatasetPartitionProducer;
import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.UserServiceImpl;
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
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class InitialLoadTest {

    @Autowired
    public DatasetPartitionProducer partitionerProducer;

    @Autowired
    public DatasetPartitionConsumer partitionerConsumer;

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

    private IUserService iUserService = new UserServiceImpl();
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
        List<DatasetPartitionMessage> datasetPartitionMessages = partitionerConsumer.retrieveMessages();
        Assertions.assertThat(iUserService.findAllUsers().size() == datasetPartitionMessages.size());
    }

    private void flushData() {
        int partitionCounter = 1;
        for (String user : iUserService.findAllUsers()) {
            DatasetPartitionMessage message = DatasetPartitionMessage.getBuilder()
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
