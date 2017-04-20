package com.umantis.poc;

import com.umantis.poc.partitioner.IUserService;
import com.umantis.poc.partitioner.KafkaUserCustomPatitioner;
import com.umantis.poc.partitioner.UserServiceImpl;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author David Espinosa.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaApplicationTests {

    @Autowired
    public KafkaProducer<String, String> producer;

    @Autowired
    public KafkaConsumer consumer;

    @Autowired
    public KafkaPartitionerProducer partitionerProducer;

    private static String TOPIC;

    @Value("${kafka.topic}")
    public void setTopic(String topic) {
        TOPIC = topic;
    }

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    // uncomment if no real kafka is running
    //    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TOPIC);

    // uncomment if no real kafka is running
    //    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty("kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
    }

    // uncomment if no real kafka is running
    //    @Before
    public void setUp() throws Exception {
        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                                                 embeddedKafka.getPartitionsPerTopic());
        }
    }

    @Test()
    public void testReceive() throws Exception {
        IUserService iUserService = new UserServiceImpl();
        for (String user : iUserService.findAllUsers()) {
            String msg = "Hello " + user;
            partitionerProducer.send(TOPIC, user, msg);
        }
        partitionerProducer.close();
        consumer.latch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.latch().getCount()).isEqualTo(0);
    }

    @Test()
    public void testReceiveBack() throws Exception {
        IUserService iUserService = new UserServiceImpl();
        for (String user : iUserService.findAllUsers()) {
            String msg = "Hello " + user;
            producer.send(new ProducerRecord<String, String>(TOPIC, user, msg), new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent:" + msg + ", User: " + user + ", Partition: " + metadata.partition());
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
        consumer.latch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.latch().getCount()).isEqualTo(0);
    }

    @Test
    public void testToDelete() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class",
                  KafkaUserCustomPatitioner.class);
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        System.out.println("Produces 5 messages");
        IUserService userService = new UserServiceImpl();
        List<String> users = userService.findAllUsers();
        for (String user : users) {

            String msg = "Hello " + user;
            myProducer.send(new ProducerRecord<String, String>(TOPIC, user, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out
                            .println("Sent:" + msg + ", User: " + user + ", Partition: " + metadata.partition());
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        // closes producer
        producer.close();
        consumer.latch().await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(consumer.latch().getCount()).isEqualTo(0);
    }
}
