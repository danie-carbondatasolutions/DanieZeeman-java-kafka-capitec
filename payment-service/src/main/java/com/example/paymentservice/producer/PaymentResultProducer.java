package com.example.paymentservice.producer;

import com.example.paymentservice.model.PaymentResultEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentResultProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String successTopic;
    private final String failedTopic;
    private final String dlqTopic;

    public PaymentResultProducer(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${PAYMENT_SUCCESS_TOPIC:payment.succeeded}") String successTopic,
            @Value("${PAYMENT_FAILED_TOPIC:payment.failed}") String failedTopic,
            @Value("${PAYMENT_DLQ_TOPIC:payment.dlq}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.successTopic = successTopic;
        this.failedTopic = failedTopic;
        this.dlqTopic = dlqTopic;
    }

    public void sendSuccess(String orderId, int amount) {
        send(successTopic, orderId, new PaymentResultEvent(orderId, "succeeded", amount, "payment-service"));
    }

    public void sendFailure(String orderId, int amount) {
        send(failedTopic, orderId, new PaymentResultEvent(orderId, "failed", amount, "payment-service"));
    }

    public void sendToDlq(String originalPayload, String errorReason) {
        try {
            String dlqPayload = objectMapper.writeValueAsString(
                    java.util.Map.of("originalPayload", originalPayload, "errorReason", errorReason));
            kafkaTemplate.send(dlqTopic, dlqPayload);
            log.warn("Message routed to DLQ '{}': {}", dlqTopic, errorReason);
        } catch (Exception ex) {
            log.error("Failed to write to DLQ: {}", ex.getMessage());
        }
    }

    private void send(String topic, String key, PaymentResultEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Published to '{}': orderId={}, partition={}, offset={}",
                                    topic, key,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to publish to '{}' for orderId={}: {}", topic, key, ex.getMessage());
                        }
                    });
        } catch (Exception ex) {
            log.error("Serialization error for orderId={}: {}", key, ex.getMessage());
        }
    }
}
