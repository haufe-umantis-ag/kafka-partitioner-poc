package com.umantis.poc;

import com.umantis.poc.admin.KafkaAdminUtils;
import com.umantis.poc.model.BaseMessage;
import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.UserServiceImpl;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PartitionerTest {

    @Autowired
    public Producer producer;

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
