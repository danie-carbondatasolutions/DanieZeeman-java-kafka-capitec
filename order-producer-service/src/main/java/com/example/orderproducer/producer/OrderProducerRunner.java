package com.example.orderproducer.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

@Component
public class OrderProducerRunner implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String orderTopic;

    public OrderProducerRunner(KafkaTemplate<String, String> kafkaTemplate,
            @Value("${ORDER_TOPIC:order.placed}") String orderTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderTopic = orderTopic;
    }

    @Override
    public void run(String... args) {
        System.out.println("[OrderProducer] Application started. Publishing order.placed events.");

        Random random = new Random();
        int count = 1;

        while (!Thread.currentThread().isInterrupted()) {
            String orderId = String.valueOf(count++);
            String payload = String.format(
                    "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"amount\":%d,\"createdAt\":\"%s\"}",
                    orderId,
                    UUID.randomUUID(),
                    20 + random.nextInt(180),
                    Instant.now().toString());

            // TODO: Evaluate whether orderId is the best key for partition affinity and
            // out-of-order handling.
            try {
                kafkaTemplate.send(orderTopic, orderId, payload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                System.out.println("Sent order.placed event for order " + orderId);
                            } else {
                                System.err.println("Failed to send order.placed event for order "
                                        + orderId + ": " + ex.getMessage());
                            }
                        });
            } catch (RuntimeException ex) {
                System.err.println("Kafka producer is not ready for order " + orderId + ": "
                        + ex.getMessage());
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
