package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.UserServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.concurrent.TimeUnit;

/**
 * Basic partitioner test that checks that sent messages are stored in different partitions
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AutomaticPartitionerTest {

    @Autowired
    public PartitionerProducer producer;

    @Autowired
    public Consumer consumer;

    @Autowired
    public KafkaAdminUtils kafkaAdminUtils;

    private static String TOPIC;

    @Value("${kafka.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
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
        consumer.latch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.latch().getCount()).isEqualTo(0);
    }
}
