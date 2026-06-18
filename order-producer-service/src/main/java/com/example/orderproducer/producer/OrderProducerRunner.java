package com.example.orderproducer.producer;

import com.example.orderproducer.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

@Component
public class OrderProducerRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerRunner.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String orderTopic;
    private final long intervalMs;

    public OrderProducerRunner(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${ORDER_TOPIC:order.placed}") String orderTopic,
            @Value("${producer.interval-ms:2000}") long intervalMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.orderTopic = orderTopic;
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(String... args) {
        log.info("OrderProducer started. Publishing to topic '{}'", orderTopic);

        Random random = new Random();
        int count = 1;

        while (!Thread.currentThread().isInterrupted()) {
            String orderId = String.valueOf(count++);
            OrderEvent order = new OrderEvent(
                    orderId,
                    UUID.randomUUID().toString(),
                    20 + random.nextInt(180),
                    Instant.now().toString());

            try {
                String payload = objectMapper.writeValueAsString(order);

                // orderId as the partition key guarantees all events for the same order
                // land on the same partition, preserving per-order event ordering.
                kafkaTemplate.send(orderTopic, orderId, payload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.info("Sent order.placed event: orderId={}, partition={}, offset={}",
                                        orderId,
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } else {
                                log.error("Failed to send order.placed for orderId={}: {}", orderId, ex.getMessage());
                            }
                        });
            } catch (Exception ex) {
                log.error("Error producing order event for orderId={}: {}", orderId, ex.getMessage());
            }

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        log.info("OrderProducer shutting down.");
    }
}
