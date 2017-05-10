package com.umantis.poc;

import static org.springframework.util.Assert.isTrue;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.partitioner.DatasetPartitionMessage;
import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.PartitionerConsumer;
import com.umantis.poc.partitioner.PartitionerProducer;
import com.umantis.poc.partitioner.UserServiceImpl;
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
public class DatasetPartitionerTest {

    @Autowired
    public PartitionerProducer partitionerProducer;

    @Autowired
    public PartitionerConsumer partitionerConsumer;

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

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
        IUserService iUserService = new UserServiceImpl();

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

        List<DatasetPartitionMessage> datasetPartitionMessages = partitionerConsumer.retrieveMessages();
        isTrue(partitionCounter == datasetPartitionMessages.size() + 1, "Messages correctly retrieved");
    }

    @After
    public void tearDown() {
        kafkaAdminUtils.markTopicForDeletion(TOPIC);
    }
}
