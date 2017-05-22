package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaUtilsTest {

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

    private static String TOPIC;

    @Value("${incremental.partition}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Before
    public void setUp() {
        if (kafkaAdminUtils.topicExists(TOPIC)) {
            TOPIC += System.currentTimeMillis();
        }
        kafkaAdminUtils.createTopic(TOPIC, -1);
    }

    @Test
    public void given_topicWithUniquePartition_when_extended_then_topicPartitionsAreIncreasesByOne() {
        int topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(TOPIC);
        Assertions.assertThat(topicPartitionsSize == 1);

        kafkaAdminUtils.addPartition(TOPIC, 1);
        topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(TOPIC);
        Assertions.assertThat(topicPartitionsSize == 2);

        kafkaAdminUtils.addPartition(TOPIC, 1);
        topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(TOPIC);
        Assertions.assertThat(topicPartitionsSize == 3);
    }
}
