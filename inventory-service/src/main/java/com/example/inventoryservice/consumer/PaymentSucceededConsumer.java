package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.PaymentResultEvent;
import com.example.inventoryservice.service.InventoryService;
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

    @KafkaListener(topics = "${PAYMENT_SUCCESS_TOPIC:payment.succeeded}", groupId = "inventory-updater-group")
    public void listen(String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[InventoryService] Received payment.succeeded: partition={}, offset={}", partition, offset);

        try {
            PaymentResultEvent event = objectMapper.readValue(message, PaymentResultEvent.class);
            inventoryService.reserveInventory(event);
        } catch (Exception ex) {
            log.error("Failed to process payment.succeeded message. payload={}, error={}", message, ex.getMessage());
        }
    }
}
