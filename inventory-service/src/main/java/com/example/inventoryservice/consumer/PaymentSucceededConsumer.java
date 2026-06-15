package com.example.inventoryservice.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentSucceededConsumer {

    @KafkaListener(topics = "${PAYMENT_SUCCESS_TOPIC:payment.succeeded}", groupId = "inventory-updater-group")
    public void listen(String message) {
        System.out.println("[InventoryService] Received payment.succeeded: " + message);

        // TODO: Add inventory reservation and update logic, and make this idempotent.
        System.out.println("Inventory updated for payment event.");
    }
}
