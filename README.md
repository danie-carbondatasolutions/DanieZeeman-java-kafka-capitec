# Java Order Management Microservices

## Services

| Service | Consumes | Produces |
|---|---|---|
| `order-producer-service` | — | `order.placed` |
| `payment-service` | `order.placed` | `payment.succeeded`, `payment.failed`, `payment.dlq` |
| `inventory-service` | `payment.succeeded` | `inventory.dlq` |

## Event Pipeline

```
order-producer  →  order.placed  →  payment-service  →  payment.succeeded  →  inventory-service
                                                     └→  payment.failed
                                                     └→  payment.dlq  (on error / retries exhausted)
                                                                           inventory-service  →  inventory.dlq
```

## Consumer Groups and Scaling

Each service belongs to its own consumer group, which controls how Kafka distributes partition assignments during scaling:

- **`payment-processor-group`** — used by `payment-service`. If you scale this deployment to 2 replicas, Kafka assigns each replica a subset of the `order.placed` partitions. No two replicas process the same message. The `order.placed` topic should have at least as many partitions as the maximum number of replicas you intend to run.

- **`inventory-updater-group`** — used by `inventory-service`. Scales independently of the payment service. Scaling this deployment to 2 replicas distributes the `payment.succeeded` partitions across both pods. The payment and inventory groups maintain completely separate offset positions, so scaling one service has no effect on the other.

**Rule of thumb:** number of replicas ≤ number of topic partitions. Extra replicas beyond the partition count sit idle because Kafka cannot assign them a partition.

## Running locally

### Prerequisites
- Rancher Desktop running
- `kubectl` configured for Rancher Desktop context

### 1. Deploy the Kafka cluster

```bash
kubectl apply -f kafka-statefulset-new.yaml
kubectl apply -f kafkaUI.yaml
```

Wait for all 3 Kafka brokers to be ready:
```bash
kubectl get pods -l app=kafka
```

### 2. Build all services

```bash
cd order-producer-service && mvn clean package -DskipTests && cd ..
cd payment-service       && mvn clean package -DskipTests && cd ..
cd inventory-service     && mvn clean package -DskipTests && cd ..
```

### 3. Build and tag Docker images

```bash
docker build -t danie-carbondatasolutions/order-producer-service:latest order-producer-service/
docker build -t danie-carbondatasolutions/payment-service:latest         payment-service/
docker build -t danie-carbondatasolutions/inventory-service:latest       inventory-service/
```

### 4. Deploy services to Kubernetes

```bash
kubectl apply -f k8s/java-services.yaml
```

### 5. Verify all pods are running

```bash
kubectl get pods
```

### 6. View logs

```bash
kubectl logs -f deployment/java-order-producer
kubectl logs -f deployment/java-payment-service
kubectl logs -f deployment/java-inventory-service
```

### 7. Open Kafka UI

Browse to [http://localhost:30080](http://localhost:30080) to inspect topics, consumer group offsets, and messages.

## Running a single service locally (without Kubernetes)

```bash
cd order-producer-service
KAFKA_BOOTSTRAP_SERVERS=localhost:30092 mvn spring-boot:run
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:30092` | Kafka broker address |
| `ORDER_TOPIC` | `order.placed` | Topic for order events |
| `PAYMENT_SUCCESS_TOPIC` | `payment.succeeded` | Topic for successful payments |
| `PAYMENT_FAILED_TOPIC` | `payment.failed` | Topic for failed payments |
| `PAYMENT_DLQ_TOPIC` | `payment.dlq` | Dead-letter topic for payment service |
| `INVENTORY_DLQ_TOPIC` | `inventory.dlq` | Dead-letter topic for inventory service |
| `PRODUCER_INTERVAL_MS` | `2000` | Milliseconds between order publishes |

## Running tests

```bash
cd order-producer-service && mvn test && cd ..
cd payment-service       && mvn test && cd ..
cd inventory-service     && mvn test && cd ..
```
