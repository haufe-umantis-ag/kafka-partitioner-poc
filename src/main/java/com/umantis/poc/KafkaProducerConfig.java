package com.umantis.poc;

import com.umantis.poc.partitioner.KafkaUserCustomPatitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * @author David Espinosa.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.servers}")
    private String servers;

    @Bean
    public Map<String,Object> producerConfigs() {
        Map<String,Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaUserCustomPatitioner.class);

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(){
        return new KafkaTemplate<String, String>(producerFactory());
    }

    @Bean
    public KafkaProducer<String, String> producer() {
        return new KafkaProducer(producerConfigs());
    }

    @Bean
    public KafkaPartitionerProducer partitionerProducer() {
        return new KafkaPartitionerProducer(producerConfigs());
    }
}
