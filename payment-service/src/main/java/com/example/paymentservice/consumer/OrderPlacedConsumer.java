package com.example.paymentservice.consumer;

import com.example.paymentservice.model.OrderEvent;
import com.example.paymentservice.model.PaymentDecision;
import com.example.paymentservice.producer.PaymentResultProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Component
public class OrderPlacedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacedConsumer.class);

    private final PaymentResultProducer resultProducer;
    private final ObjectMapper objectMapper;
    private final Supplier<Boolean> paymentOutcomeSupplier;

    // Idempotency guard: prevents processing the same orderId twice if Kafka
    // delivers the same message more than once (at-least-once delivery guarantee).
    // NOTE: this is in-memory only — it resets on pod restart. A production system
    // would back this with Redis or a DB unique constraint.
    private final Set<String> processedOrders = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Primary constructor — uses random payment outcome
    public OrderPlacedConsumer(PaymentResultProducer resultProducer, ObjectMapper objectMapper) {
        this(resultProducer, objectMapper, () -> Math.random() > 0.5);
    }

    // Injectable constructor for testing — allows deterministic payment outcome
    public OrderPlacedConsumer(PaymentResultProducer resultProducer, ObjectMapper objectMapper,
            Supplier<Boolean> paymentOutcomeSupplier) {
        this.resultProducer = resultProducer;
        this.objectMapper = objectMapper;
        this.paymentOutcomeSupplier = paymentOutcomeSupplier;
    }

    @KafkaListener(topics = "${ORDER_TOPIC:order.placed}", groupId = "${CONSUMER_GROUP_ID:payment-processor-group}")
    public void listen(String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[PaymentService] Received order.placed: partition={}, offset={}", partition, offset);

        // Deserialization failures are not retryable — bad JSON will always be bad JSON.
        // Route directly to DLQ and commit the offset so the consumer moves on.
        OrderEvent order;
        try {
            order = objectMapper.readValue(message, OrderEvent.class);
        } catch (JsonProcessingException ex) {
            log.error("Unrecoverable deserialization error, routing to DLQ: payload={}, error={}", message, ex.getMessage());
            resultProducer.sendToDlq(message, ex.getMessage());
            return;
        }

        if (order.getOrderId() == null) {
            log.error("Order event missing orderId, routing to DLQ: payload={}", message);
            resultProducer.sendToDlq(message, "orderId is null");
            return;
        }

        if (!processedOrders.add(order.getOrderId())) {
            log.warn("Duplicate order.placed event for orderId={}, skipping.", order.getOrderId());
            return;
        }

        // Processing exceptions (e.g. downstream unavailable) are rethrown so that
        // DefaultErrorHandler can apply the configured retry backoff before routing to DLQ.
        PaymentDecision decision = simulatePayment(order.getOrderId());

        if (decision.isSuccess()) {
            log.info("Payment SUCCEEDED for orderId={}, amount={}", order.getOrderId(), order.getAmount());
            resultProducer.sendSuccess(order.getOrderId(), order.getAmount());
        } else {
            log.warn("Payment FAILED for orderId={}, amount={}", order.getOrderId(), order.getAmount());
            resultProducer.sendFailure(order.getOrderId(), order.getAmount());
        }
    }

    private PaymentDecision simulatePayment(String orderId) {
        return new PaymentDecision(orderId, paymentOutcomeSupplier.get());
    }
}
