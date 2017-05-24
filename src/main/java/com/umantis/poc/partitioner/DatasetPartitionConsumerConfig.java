package com.umantis.poc.partitioner;

import com.umantis.poc.model.BaseMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a Event Sourcing consumer.
 *
 * @author David Espinosa.
 * @author Gergely Szakács
 */
@Configuration
@EnableKafka
@DependsOn("TopicsInitializer")
public class DatasetPartitionConsumerConfig {

    @Value("${kafka.servers}")
    private String servers;

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "partitioner");

        return props;
    }

    public ConsumerFactory<String, BaseMessage> factoryConfig() {
        return new DefaultKafkaConsumerFactory<String, BaseMessage>(consumerConfigs(), new StringDeserializer(), new JsonDeserializer(DataSetPartitionMessage.class));
    }

    public ConcurrentKafkaListenerContainerFactory<String, BaseMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, BaseMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(factoryConfig());
        return factory;
    }

    @Bean
    public DataSetPartitionConsumer partitionerConsumer() {
        return new DataSetPartitionConsumer(consumerConfigs(), new StringDeserializer(), new JsonDeserializer(DataSetPartitionMessage.class));
    }
}
