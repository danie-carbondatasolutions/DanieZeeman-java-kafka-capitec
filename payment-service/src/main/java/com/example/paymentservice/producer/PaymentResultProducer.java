package com.example.paymentservice.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentResultProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String successTopic;
    private final String failedTopic;

    public PaymentResultProducer(KafkaTemplate<String, String> kafkaTemplate,
            @Value("${PAYMENT_SUCCESS_TOPIC:payment.succeeded}") String successTopic,
            @Value("${PAYMENT_FAILED_TOPIC:payment.failed}") String failedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.successTopic = successTopic;
        this.failedTopic = failedTopic;
    }

    public void sendSuccess(String orderId, String orderPayload) {
        var event = String.format("{\"orderId\":\"%s\",\"status\":\"succeeded\",\"source\":\"payment-service\"}",
                orderId);
        kafkaTemplate.send(successTopic, orderId, event);
        System.out.println("Published payment.succeeded for order " + orderId);
    }

    public void sendFailure(String orderId, String orderPayload) {
        var event = String.format("{\"orderId\":\"%s\",\"status\":\"failed\",\"source\":\"payment-service\"}", orderId);
        kafkaTemplate.send(failedTopic, orderId, event);
        System.out.println("Published payment.failed for order " + orderId);
    }
}
