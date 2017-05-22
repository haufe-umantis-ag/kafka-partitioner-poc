package com.umantis.poc.config;

import com.umantis.poc.admin.KafkaAdminUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The main point of this component is to initialize topics before exisitins listeners and produced are loaded into the context, as this way
 * we have observed errors at first message consuming.
 * @author David Espinosa.
 */
@Component("TopicsInitializer")
public class TopicsInitializer {

    private KafkaAdminUtils kafkaAdminUtils;

    private String usersTopic;

    private String partitionsTopic;

    @Autowired
    public TopicsInitializer(final KafkaAdminUtils kafkaAdminUtils, @Value("${partition.topic}") String partitionsTopic, @Value("${kafka.topic}") String usersTopic) {
        this.kafkaAdminUtils = kafkaAdminUtils;
        this.partitionsTopic = partitionsTopic;
        this.usersTopic = usersTopic;
        initializeTopics();
    }

    private void initializeTopics() {
        if (!kafkaAdminUtils.topicExists(usersTopic)) {
            kafkaAdminUtils.createTopic(usersTopic, -1);
        }

        if (!kafkaAdminUtils.topicExists(partitionsTopic)) {
            kafkaAdminUtils.createTopic(partitionsTopic, -1);
        }
    }
}
