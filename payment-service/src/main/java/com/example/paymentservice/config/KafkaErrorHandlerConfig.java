package com.example.paymentservice.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.function.BiFunction;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);

    @Value("${PAYMENT_DLQ_TOPIC:payment.dlq}")
    private String dlqTopic;

    /**
     * Retries a failed message up to 3 times with a 1-second delay between attempts.
     * After all retries are exhausted, the message is published to the DLQ topic
     * and the offset is committed so the consumer moves on.
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (BiFunction<ConsumerRecord<?, ?>, Exception, org.apache.kafka.common.TopicPartition>)
                        (record, ex) -> {
                            log.error("Message exhausted retries, routing to DLQ '{}': key={}, error={}",
                                    dlqTopic, record.key(), ex.getMessage());
                            return new org.apache.kafka.common.TopicPartition(dlqTopic, 0);
                        });

        // 3 retries, 1 second apart
        FixedBackOff backOff = new FixedBackOff(1000L, 3);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
