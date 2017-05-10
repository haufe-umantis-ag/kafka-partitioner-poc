package com.umantis.poc.config;

import com.umantis.poc.Consumer;
import com.umantis.poc.model.BaseMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author David Espinosa.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.servers}")
    private String servers;

    @Value("${consumer.grouip}")
    private String groupId;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return props;
    }

    @Bean
    public ConsumerFactory<String, BaseMessage> factoryConfig() {
        return new DefaultKafkaConsumerFactory<String, BaseMessage>(consumerConfigs(), new StringDeserializer(), new JsonDeserializer(BaseMessage.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BaseMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, BaseMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(factoryConfig());
        return factory;
    }

    @Bean
    public Consumer consumer() {
        return new Consumer();
    }
}
