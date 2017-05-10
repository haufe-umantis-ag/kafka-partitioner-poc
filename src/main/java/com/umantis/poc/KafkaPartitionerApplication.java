package com.umantis.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author David Espinosa.
 */
@SpringBootApplication
public class KafkaPartitionerApplication {

    public static void main(String args[]) {
        SpringApplication.run(KafkaPartitionerApplication.class, args);
    }
}
