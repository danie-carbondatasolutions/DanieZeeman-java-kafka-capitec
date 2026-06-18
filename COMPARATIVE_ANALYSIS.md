# Comparative Analysis: Java vs .NET Kafka Implementations

## Overview

This document compares how the Java (Spring Boot + Spring Kafka) and .NET (ASP.NET Core + Confluent.Kafka) implementations of the same event-driven order management system differ across three key dimensions: client configuration, data serialization, and error management. It also covers consumer group scaling behaviour, which differs meaningfully between the two platforms.

---

## 1. Client Configuration

### Java (Spring Kafka)

Spring Kafka wraps the Apache Kafka Java client and integrates deeply with Spring Boot's auto-configuration system. Configuration is driven by `application.properties` or `application.yml`:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.properties.linger.ms=20
spring.kafka.producer.properties.batch.size=32768
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.listener.ack-mode=record
```

- **Auto-configuration**: Spring Boot creates `KafkaTemplate`, `KafkaListenerContainerFactory`, and all underlying producers/consumers automatically from properties.
- **Listener registration**: Consumers are declared with `@KafkaListener(topics = "...")` annotations — no manual poll loop needed.
- **Concurrency**: Controlled via `spring.kafka.listener.concurrency` — each concurrent listener gets its own thread and partition assignment.
- **Producer send**: `KafkaTemplate.send()` returns a `CompletableFuture`, enabling async callbacks with `.whenComplete()`.

### .NET (Confluent.Kafka)

The .NET Confluent client is a lower-level wrapper around `librdkafka`. Configuration is done programmatically via `ProducerConfig` and `ConsumerConfig` objects:

```csharp
var producerConfig = new ProducerConfig {
    BootstrapServers = "localhost:30092",
    Acks = Acks.All,
    LingerMs = 20,
    BatchSize = 32768,
    EnableIdempotence = true
};
var producer = new ProducerBuilder<string, string>(producerConfig).Build();
```

- **Manual wiring**: No auto-configuration; producers and consumers must be explicitly created and registered in the DI container.
- **Poll loop**: The consumer requires an explicit `while (true) { consumer.Consume(timeout) }` loop, giving the developer full control over throughput and cancellation.
- **Hosted Services**: The idiomatic .NET pattern is to run Kafka consumers inside `IHostedService` / `BackgroundService` implementations.
- **Async model**: The .NET producer's `ProduceAsync()` returns a `Task<DeliveryResult>`, fitting naturally into C#'s `async/await` model.

### Key Difference

Java/Spring Kafka abstracts away the poll loop entirely — `@KafkaListener` does it for you. .NET gives you the raw poll loop, which is more verbose but offers finer control over consumer lifecycle and error handling.

---

## 2. Consumer Groups and Scaling

This is one of the most practically important topics for production deployments.

### Java (Spring Kafka)

Consumer groups are declared directly on the `@KafkaListener` annotation:

```java
@KafkaListener(topics = "order.placed", groupId = "payment-processor-group")
public void listen(String message) { ... }
```

In this project:
- `payment-processor-group` — all replicas of `payment-service` join this group
- `inventory-updater-group` — all replicas of `inventory-service` join this group

**How scaling works:** When the `payment-service` Kubernetes Deployment is scaled to 2 replicas, Kafka's group coordinator rebalances the `order.placed` partitions between the two pods. Each pod receives an exclusive subset of partitions — no two pods process the same message. If the topic has 3 partitions and you run 3 replicas, each replica handles exactly one partition. A 4th replica would sit idle because there are no remaining partitions to assign.

Concurrency within a single pod is controlled by:
```properties
spring.kafka.listener.concurrency=3
```
This spawns 3 consumer threads inside the pod, each assigned a partition — equivalent to running 3 pods for partition distribution purposes.

The two groups (`payment-processor-group` and `inventory-updater-group`) are completely independent. Each maintains its own committed offset on its respective topic. Scaling the payment service does not affect the inventory service's offset or partition assignment.

### .NET (Confluent.Kafka)

Consumer groups work identically at the Kafka protocol level. The difference is in how they are expressed:

```csharp
var consumerConfig = new ConsumerConfig {
    BootstrapServers = "localhost:30092",
    GroupId = "payment-processor-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};
using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe("order.placed");

while (!cancellationToken.IsCancellationRequested) {
    var result = consumer.Consume(cancellationToken);
    Process(result.Message.Value);
    consumer.Commit(result);
}
```

Scaling a .NET service in Kubernetes triggers the same Kafka rebalance — each pod joins the group with its own consumer instance, and Kafka redistributes partitions. The developer must ensure that each pod creates exactly one consumer per group and subscribes before entering the poll loop.

### Key Difference

Spring Kafka's `@KafkaListener` and `concurrency` property handle the threading model automatically — the framework creates the right number of consumer threads and manages the poll loop. In .NET, the developer must explicitly construct each consumer, manage the poll loop, and ensure clean shutdown on `CancellationToken` cancellation. Java is safer by default; .NET gives explicit control over every thread boundary.

---

## 3. Data Serialization

### Java

Spring Kafka uses `StringSerializer` / `StringDeserializer` by default for simple string messages. For structured data, Jackson (`jackson-databind`) is the standard choice:

```java
// Serialization
String payload = objectMapper.writeValueAsString(orderEvent);
kafkaTemplate.send(topic, key, payload);

// Deserialization
OrderEvent order = objectMapper.readValue(message, OrderEvent.class);
```

- **POJO mapping**: Jackson maps JSON fields to Java class fields using getter/setter conventions or annotations (`@JsonProperty`).
- **Spring Kafka native JSON support**: Spring Kafka also offers `JsonSerializer` / `JsonDeserializer` that embed the type header in the Kafka message, enabling fully automatic deserialization without manual `ObjectMapper` calls.
- **Schema flexibility**: Avro + Confluent Schema Registry integration is available via `spring-kafka` + `io.confluent:kafka-avro-serializer`.

### .NET

The Confluent client uses a similar approach — `StringSerializer` for plain strings, and manual JSON serialization via `System.Text.Json` or `Newtonsoft.Json`:

```csharp
// Serialization
string payload = JsonSerializer.Serialize(orderEvent);
await producer.ProduceAsync(topic, new Message<string, string> { Key = orderId, Value = payload });

// Deserialization
var order = JsonSerializer.Deserialize<OrderEvent>(message.Value);
```

- **Custom serializers**: The .NET client supports `ISerializer<T>` / `IDeserializer<T>` interfaces for type-safe, generic producers/consumers (`IProducer<string, OrderEvent>`).
- **Avro support**: Available via `Confluent.SchemaRegistry.Serdes.Avro`.
- **Records**: C# records (`record OrderEvent(string OrderId, ...)`) pair well with `System.Text.Json` for clean, immutable message models.

### Key Difference

Java/Spring Kafka offers more out-of-the-box JSON automation (embedded type headers, auto-deserialisation). .NET requires slightly more wiring but the generic `IProducer<TKey, TValue>` / `IConsumer<TKey, TValue>` interface gives compile-time type safety across the entire messaging layer.

---

## 4. Error Management

### Java — DefaultErrorHandler and @RetryableTopic

Spring Kafka provides two distinct retry mechanisms:

**Option A — `DefaultErrorHandler` (used in this project):**

```java
@Bean
public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
        (record, ex) -> new TopicPartition("payment.dlq", 0));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
}
```

- Retries the failed message in-place up to 3 times with a 1-second pause between attempts.
- After retries are exhausted, `DeadLetterPublishingRecoverer` publishes the message to the DLQ topic.
- The consumer thread is **blocked** during the retry wait — this is simple but reduces throughput.

**Option B — `@RetryableTopic` (non-blocking retry):**

```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 1000, multiplier = 2),
    dltTopicSuffix = ".dlq"
)
@KafkaListener(topics = "order.placed", groupId = "payment-processor-group")
public void listen(String message) { ... }
```

- Retries are routed to intermediate retry topics (`order.placed-retry-0`, `order.placed-retry-1`) rather than blocking the main thread.
- The original consumer continues processing other messages during the backoff window.
- After all retry topics are exhausted, the message lands on `order.placed.dlq` automatically.
- This is the preferred pattern for high-throughput services where blocking the consumer thread for retries is unacceptable.

**Key distinction:** `DefaultErrorHandler` blocks the consumer thread during backoff; `@RetryableTopic` does not — it re-enqueues into retry topics and frees the thread immediately. `.NET` has no equivalent to either — all retry logic must be hand-coded.

### .NET

The Confluent .NET client has no built-in retry/DLQ framework — error handling is entirely the developer's responsibility:

```csharp
try {
    var result = consumer.Consume(cancellationToken);
    ProcessMessage(result.Message.Value);
    consumer.Commit(result);
} catch (ConsumeException ex) {
    logger.LogError("Consume error: {Reason}", ex.Error.Reason);
    await producer.ProduceAsync("order.placed.dlq",
        new Message<string, string> { Value = ex.Message.ToString() });
} catch (Exception ex) {
    logger.LogError("Processing error: {Message}", ex.Message);
    // Polly retry policy or manual DLQ
}
```

- **No built-in retry**: Developers implement retry logic using Polly (`Microsoft.Extensions.Http.Polly`) or custom loops.
- **Manual DLQ**: DLQ publishing must be coded explicitly.
- **Offset management**: On error, you choose whether to commit the offset (skip) or not commit (reprocess on restart). Java's `ack-mode=record` gives the same per-record control.

### Key Difference

Spring Kafka's `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` provides production-ready retry and DLQ behaviour with minimal configuration. `@RetryableTopic` goes further — it achieves non-blocking retry by routing through intermediate topics, something .NET has no out-of-the-box equivalent for and requires significant custom infrastructure to replicate.

---

## Summary Table

| Dimension | Java / Spring Kafka | .NET / Confluent.Kafka |
|---|---|---|
| Configuration style | `application.properties` + auto-config | `ProducerConfig` / `ConsumerConfig` objects in code |
| Consumer registration | `@KafkaListener` annotation | Manual `Consume()` loop in `BackgroundService` |
| In-pod concurrency | `spring.kafka.listener.concurrency=N` | One consumer per `BackgroundService` instance |
| Consumer group scaling | Automatic rebalance; replicas ≤ partition count | Same Kafka protocol; developer manages consumer lifecycle |
| Serialization default | `StringSerializer` + Jackson POJO | `StringSerializer` + `System.Text.Json` / Newtonsoft |
| Type-safe generics | `KafkaTemplate<K,V>` | `IProducer<K,V>` / `IConsumer<K,V>` |
| Blocking retry | `DefaultErrorHandler` + `FixedBackOff` | Manual via Polly |
| Non-blocking retry | `@RetryableTopic` (intermediate retry topics) | No built-in equivalent |
| DLQ support | `DeadLetterPublishingRecoverer` (automatic) | Manual producer send in catch block |
| Idempotent producer | `enable.idempotence=true` in properties | `EnableIdempotence = true` in `ProducerConfig` |
| Async model | `CompletableFuture` + `.whenComplete()` | `Task<DeliveryResult>` + `async/await` |

---

## Conclusion

Both stacks implement the same event-driven patterns, but at different levels of abstraction. Spring Kafka's opinionated auto-configuration, built-in retry mechanisms (`DefaultErrorHandler`, `@RetryableTopic`), and automatic consumer group management reduce boilerplate significantly and make production-grade consumers faster to build and safer by default.

The Confluent .NET client is lower-level and more explicit — it requires more code for the same resilience guarantees, but in return gives developers full visibility and control over every step of the consume-process-commit lifecycle. The `@RetryableTopic` pattern in particular has no direct .NET equivalent and represents a meaningful ergonomic advantage for Java teams dealing with high-throughput, fault-tolerant consumers.

For teams already invested in the Spring ecosystem, the Java approach is more productive. For .NET teams, the explicit control is idiomatic and well-suited to `async/await`-heavy microservices, provided the team is prepared to build retry and DLQ infrastructure from scratch.
