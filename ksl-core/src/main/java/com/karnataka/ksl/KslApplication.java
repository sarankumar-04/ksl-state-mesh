package com.karnataka.ksl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * K-State Ledger (KSL) — Non-Invasive State Mesh
 *
 * NOTE: @EnableKafka is intentionally NOT here.
 * It lives on KafkaConfig which is @ConditionalOnProperty,
 * so Kafka only activates when spring.kafka.bootstrap-servers is set.
 * In sandbox profile → zero Kafka, zero spam logs.
 */
@SpringBootApplication
@EnableScheduling
public class KslApplication {

    public static void main(String[] args) {
        SpringApplication.run(KslApplication.class, args);
    }
}
