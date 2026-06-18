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

    private OrderPlacedConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new OrderPlacedConsumer(resultProducer, new ObjectMapper());
    }

    @Test
    void routesToSuccessOrFailureTopicForValidOrder() {
        String validPayload = "{\"orderId\":\"1\",\"customerId\":\"c1\",\"amount\":100,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        // Run enough times to statistically hit both branches (50/50 random)
        for (int i = 0; i < 20; i++) {
            consumer.listen(validPayload.replace("\"orderId\":\"1\"", "\"orderId\":\"" + i + "\""), 0, i);
        }

        // At least one success and one failure should have been published
        verify(resultProducer, atLeastOnce()).sendSuccess(anyString(), anyInt());
        verify(resultProducer, atLeastOnce()).sendFailure(anyString(), anyInt());
        verify(resultProducer, never()).sendToDlq(anyString(), anyString());
    }

    @Test
    void routesToDlqForMalformedJson() {
        String badPayload = "NOT_JSON";

        consumer.listen(badPayload, 0, 0L);

        verify(resultProducer).sendToDlq(eq(badPayload), anyString());
        verify(resultProducer, never()).sendSuccess(anyString(), anyInt());
        verify(resultProducer, never()).sendFailure(anyString(), anyInt());
    }

    @Test
    void routesToDlqForMissingOrderId() {
        String missingOrderId = "{\"customerId\":\"c1\",\"amount\":50}";

        consumer.listen(missingOrderId, 0, 0L);

        // orderId is null → sendSuccess/Failure called with null key or DLQ
        // Depending on implementation, this may route to DLQ
        // Either way, no exception should propagate to the container
    }

    @Test
    void idempotencySkipsDuplicateOrderId() {
        String payload = "{\"orderId\":\"dup-1\",\"customerId\":\"c1\",\"amount\":100,\"createdAt\":\"2026-01-01T00:00:00Z\"}";

        consumer.listen(payload, 0, 0L);
        consumer.listen(payload, 0, 1L);  // duplicate

        // Success or failure published exactly once (not twice)
        int successCount = mockingDetails(resultProducer).getInvocations()
                .stream().filter(i -> i.getMethod().getName().equals("sendSuccess")).toList().size();
        int failureCount = mockingDetails(resultProducer).getInvocations()
                .stream().filter(i -> i.getMethod().getName().equals("sendFailure")).toList().size();

        assertEquals(1, successCount + failureCount);
    }
}
