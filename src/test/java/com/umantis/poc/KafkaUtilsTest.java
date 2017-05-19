package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.assertj.core.api.Assertions;
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

    @Value("${partition.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Test
    public void getTopicPartionsTest() {
        int topicPartitionsSize = kafkaAdminUtils.getTopicPartitionsSize(TOPIC);
        Assertions.assertThat(topicPartitionsSize==0);
    }
}
