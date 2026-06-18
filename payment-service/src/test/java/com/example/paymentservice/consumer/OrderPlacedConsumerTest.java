package com.example.paymentservice.consumer;

import com.example.paymentservice.producer.PaymentResultProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderPlacedConsumerTest {

    @Mock
    private PaymentResultProducer resultProducer;

    private OrderPlacedConsumer successConsumer;
    private OrderPlacedConsumer failureConsumer;
    private OrderPlacedConsumer randomConsumer;

    @BeforeEach
    void setUp() {
        // Deterministic consumers — no Math.random() in tests
        successConsumer = new OrderPlacedConsumer(resultProducer, new ObjectMapper(), () -> true);
        failureConsumer = new OrderPlacedConsumer(resultProducer, new ObjectMapper(), () -> false);
        randomConsumer  = new OrderPlacedConsumer(resultProducer, new ObjectMapper());
    }

    @Test
    void routesToSuccessTopicWhenPaymentSucceeds() {
        String payload = "{\"orderId\":\"1\",\"customerId\":\"c1\",\"amount\":100,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        successConsumer.listen(payload, 0, 0L);

        verify(resultProducer).sendSuccess("1", 100);
        verify(resultProducer, never()).sendFailure(anyString(), anyInt());
        verify(resultProducer, never()).sendToDlq(anyString(), anyString());
    }

    @Test
    void routesToFailureTopicWhenPaymentFails() {
        String payload = "{\"orderId\":\"2\",\"customerId\":\"c1\",\"amount\":50,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        failureConsumer.listen(payload, 0, 0L);

        verify(resultProducer).sendFailure("2", 50);
        verify(resultProducer, never()).sendSuccess(anyString(), anyInt());
        verify(resultProducer, never()).sendToDlq(anyString(), anyString());
    }

    @Test
    void routesToDlqForMalformedJson() {
        String badPayload = "NOT_JSON";

        successConsumer.listen(badPayload, 0, 0L);

        verify(resultProducer).sendToDlq(eq(badPayload), anyString());
        verify(resultProducer, never()).sendSuccess(anyString(), anyInt());
        verify(resultProducer, never()).sendFailure(anyString(), anyInt());
    }

    @Test
    void routesToDlqForMissingOrderId() {
        successConsumer.listen("{}", 0, 0L);

        // orderId is null — consumer validates and routes to DLQ before payment logic runs
        verify(resultProducer).sendToDlq(eq("{}"), contains("orderId is null"));
        verify(resultProducer, never()).sendSuccess(anyString(), anyInt());
        verify(resultProducer, never()).sendFailure(anyString(), anyInt());
    }

    @Test
    void idempotencySkipsDuplicateOrderId() {
        String payload = "{\"orderId\":\"dup-1\",\"customerId\":\"c1\",\"amount\":100,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        successConsumer.listen(payload, 0, 0L);
        successConsumer.listen(payload, 0, 1L); // duplicate — same orderId

        // sendSuccess called exactly once despite two deliveries
        verify(resultProducer, times(1)).sendSuccess(eq("dup-1"), eq(100));
    }

    @Test
    void processesDistinctOrderIdsIndependently() {
        String payload1 = "{\"orderId\":\"A\",\"customerId\":\"c1\",\"amount\":10,\"createdAt\":\"2026-01-01T00:00:00Z\"}";
        String payload2 = "{\"orderId\":\"B\",\"customerId\":\"c2\",\"amount\":20,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        successConsumer.listen(payload1, 0, 0L);
        successConsumer.listen(payload2, 0, 1L);

        verify(resultProducer).sendSuccess("A", 10);
        verify(resultProducer).sendSuccess("B", 20);
    }
}
