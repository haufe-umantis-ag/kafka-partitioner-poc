package com.umantis.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring boot application main.
 *
 * @author David Espinosa.
 * @author Gergely Szakács
 */
@SpringBootApplication
public class KafkaPartitionerApplication {

    public static void main(String args[]) {
        SpringApplication.run(KafkaPartitionerApplication.class, args);
    }
}
