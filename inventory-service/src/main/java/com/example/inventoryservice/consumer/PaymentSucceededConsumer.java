package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.PaymentResultEvent;
import com.example.inventoryservice.service.InventoryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class PaymentSucceededConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentSucceededConsumer.class);

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    public PaymentSucceededConsumer(InventoryService inventoryService, ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${PAYMENT_SUCCESS_TOPIC:payment.succeeded}", groupId = "${CONSUMER_GROUP_ID:inventory-updater-group}")
    public void listen(String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[InventoryService] Received payment.succeeded: partition={}, offset={}", partition, offset);

        // Deserialization failures are not retryable — route directly to DLQ.
        PaymentResultEvent event;
        try {
            event = objectMapper.readValue(message, PaymentResultEvent.class);
        } catch (JsonProcessingException ex) {
            log.error("Unrecoverable deserialization error, routing to DLQ: payload={}, error={}", message, ex.getMessage());
            // Rethrow as RuntimeException — DefaultErrorHandler will route to inventory.dlq
            throw new IllegalArgumentException("Unparseable payment.succeeded message: " + ex.getMessage(), ex);
        }

        // Processing exceptions propagate to DefaultErrorHandler for retry + DLQ routing
        inventoryService.reserveInventory(event);
    }
}
