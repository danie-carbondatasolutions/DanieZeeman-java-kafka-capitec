package com.example.paymentservice.consumer;

import com.example.paymentservice.model.OrderEvent;
import com.example.paymentservice.model.PaymentDecision;
import com.example.paymentservice.producer.PaymentResultProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderPlacedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacedConsumer.class);

    private final PaymentResultProducer resultProducer;
    private final ObjectMapper objectMapper;

    // Idempotency guard: prevents processing the same orderId twice if Kafka
    // delivers the same message more than once (at-least-once delivery guarantee).
    private final Set<String> processedOrders = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public OrderPlacedConsumer(PaymentResultProducer resultProducer, ObjectMapper objectMapper) {
        this.resultProducer = resultProducer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${ORDER_TOPIC:order.placed}", groupId = "payment-processor-group")
    public void listen(String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[PaymentService] Received order.placed: partition={}, offset={}, payload={}", partition, offset, message);

        try {
            OrderEvent order = objectMapper.readValue(message, OrderEvent.class);

            if (!processedOrders.add(order.getOrderId())) {
                log.warn("Duplicate order.placed event for orderId={}, skipping.", order.getOrderId());
                return;
            }

            PaymentDecision decision = simulatePayment(order.getOrderId());

            if (decision.isSuccess()) {
                log.info("Payment SUCCEEDED for orderId={}, amount={}", order.getOrderId(), order.getAmount());
                resultProducer.sendSuccess(order.getOrderId(), order.getAmount());
            } else {
                log.warn("Payment FAILED for orderId={}, amount={}", order.getOrderId(), order.getAmount());
                resultProducer.sendFailure(order.getOrderId(), order.getAmount());
            }

        } catch (Exception ex) {
            log.error("Failed to process order.placed message, routing to DLQ. payload={}, error={}", message, ex.getMessage());
            resultProducer.sendToDlq(message, ex.getMessage());
        }
    }

    private PaymentDecision simulatePayment(String orderId) {
        boolean success = Math.random() > 0.5;
        return new PaymentDecision(orderId, success);
    }
}
