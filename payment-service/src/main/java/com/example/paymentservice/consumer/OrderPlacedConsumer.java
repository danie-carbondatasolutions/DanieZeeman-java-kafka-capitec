package com.example.paymentservice.consumer;

import com.example.paymentservice.producer.PaymentResultProducer;
import com.example.paymentservice.model.PaymentDecision;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderPlacedConsumer {

    private final PaymentResultProducer resultProducer;

    public OrderPlacedConsumer(PaymentResultProducer resultProducer) {
        this.resultProducer = resultProducer;
    }

    @KafkaListener(topics = "${ORDER_TOPIC:order.placed}", groupId = "payment-processor-group")
    public void listen(String message) {
        System.out.println("[PaymentService] Received order.placed: " + message);

        // TODO: Deserialize the order payload and calculate the payment amount.
        var decision = simulatePayment();

        if (decision.isSuccess()) {
            resultProducer.sendSuccess(decision.getOrderId(), message);
        } else {
            resultProducer.sendFailure(decision.getOrderId(), message);
        }
    }

    private PaymentDecision simulatePayment() {
        var orderId = java.util.UUID.randomUUID().toString();
        var success = Math.random() > 0.5;
        return new PaymentDecision(orderId, success);
    }
}
