package com.example.orderproducer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProducerRunnerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void sendsMessageWithOrderIdAsKey() throws Exception {
        RecordMetadata meta = new RecordMetadata(new TopicPartition("order.placed", 0), 0, 0, 0, 0, 0);
        SendResult<String, String> sendResult = new SendResult<>(new ProducerRecord<>("order.placed", "1", "{}"), meta);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        OrderProducerRunner runner = new OrderProducerRunner(kafkaTemplate, new ObjectMapper(), "order.placed", 0L);

        Thread thread = new Thread(() -> {
            try { runner.run(); } catch (Exception ignored) {}
        });
        thread.start();
        Thread.sleep(50);
        thread.interrupt();
        thread.join(500);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("order.placed"), keyCaptor.capture(), valueCaptor.capture());

        String key = keyCaptor.getValue();
        String payload = valueCaptor.getValue();
        assertNotNull(key);
        assertTrue(payload.contains("\"orderId\":\"" + key + "\""), "orderId in payload must match the message key");
        assertTrue(payload.contains("\"customerId\""));
        assertTrue(payload.contains("\"amount\""));
        assertTrue(payload.contains("\"createdAt\""));
    }

    @Test
    void payloadIsValidJson() throws Exception {
        RecordMetadata meta = new RecordMetadata(new TopicPartition("order.placed", 0), 0, 0, 0, 0, 0);
        SendResult<String, String> sendResult = new SendResult<>(new ProducerRecord<>("order.placed", "1", "{}"), meta);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        OrderProducerRunner runner = new OrderProducerRunner(kafkaTemplate, new ObjectMapper(), "order.placed", 0L);
        Thread thread = new Thread(() -> { try { runner.run(); } catch (Exception ignored) {} });
        thread.start();
        Thread.sleep(50);
        thread.interrupt();
        thread.join(500);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString(), valueCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        assertDoesNotThrow(() -> mapper.readTree(valueCaptor.getValue()), "Payload must be valid JSON");
    }
}
