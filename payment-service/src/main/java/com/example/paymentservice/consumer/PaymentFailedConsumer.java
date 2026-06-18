package com.example.paymentservice.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedConsumer.class);

    @KafkaListener(topics = "${PAYMENT_FAILED_TOPIC:payment.failed}", groupId = "${CONSUMER_GROUP_ID:payment-processor-group}-failed")
    public void listen(String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.warn("[PaymentService] Payment failed event received: partition={}, offset={}, payload={}",
                partition, offset, message);

        // Extension point: notify customer, trigger retry workflow, or escalate to support.
        // Currently logs the failure for audit purposes.
        log.warn("Order payment failed — no automated recovery configured. Manual review required.");
    }
}
