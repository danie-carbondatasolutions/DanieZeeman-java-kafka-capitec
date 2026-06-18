package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.PaymentResultEvent;
import com.example.inventoryservice.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentSucceededConsumerTest {

    @Mock
    private InventoryService inventoryService;

    private PaymentSucceededConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PaymentSucceededConsumer(inventoryService, new ObjectMapper());
    }

    @Test
    void delegatesValidEventToInventoryService() {
        String payload = "{\"orderId\":\"order-1\",\"status\":\"succeeded\",\"amount\":120,\"source\":\"payment-service\"}";

        consumer.listen(payload, 0, 0L);

        ArgumentCaptor<PaymentResultEvent> captor = ArgumentCaptor.forClass(PaymentResultEvent.class);
        verify(inventoryService).reserveInventory(captor.capture());

        PaymentResultEvent event = captor.getValue();
        assertEquals("order-1", event.getOrderId());
        assertEquals("succeeded", event.getStatus());
        assertEquals(120, event.getAmount());
    }

    @Test
    void doesNotPropagateExceptionOnMalformedJson() {
        // Consumer must not crash the container on bad input
        assertDoesNotThrow(() -> consumer.listen("INVALID_JSON", 0, 0L));
        verify(inventoryService, never()).reserveInventory(any());
    }

    @Test
    void doesNotPropagateExceptionWhenInventoryServiceThrows() {
        String payload = "{\"orderId\":\"order-1\",\"status\":\"succeeded\",\"amount\":50,\"source\":\"payment-service\"}";
        doThrow(new RuntimeException("DB unavailable")).when(inventoryService).reserveInventory(any());

        assertDoesNotThrow(() -> consumer.listen(payload, 0, 0L));
    }
}
