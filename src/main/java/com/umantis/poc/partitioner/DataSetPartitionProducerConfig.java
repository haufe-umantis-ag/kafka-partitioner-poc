package com.umantis.poc.partitioner;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 * Base producer configuration
 * @author David Espinosa.
 */
@Configuration
@DependsOn("TopicsInitializer")
public class DataSetPartitionProducerConfig {

    @Value("${kafka.servers}")
    private String servers;

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        return props;
    }

    public ProducerFactory<String, DataSetPartitionMessage> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), new JsonSerializer(new ObjectMapper()));
    }

    public KafkaTemplate<String, DataSetPartitionMessage> kafkaTemplate() {
        return new KafkaTemplate<String, DataSetPartitionMessage>(producerFactory());
    }

    @Bean
    public DataSetPartitionerProducer partitionerProducer() {
        return new DataSetPartitionerProducer(kafkaTemplate());
    }
}
