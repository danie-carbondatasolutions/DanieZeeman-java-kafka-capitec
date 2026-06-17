# Java Order Management Microservices

## Services

- `order-producer-service`: publishes `order.placed`
- `payment-service`: consumes `order.placed`, produces `payment.succeeded` or `payment.failed`
- `inventory-service`: consumes `payment.succeeded` and logs inventory updates

## Running locally

1. Run the Kafka cluster with `kafka-statefulset-new.yaml` on Rancher Desktop.
2. From each service folder, start the service with Maven:

```bash
cd java-order-service/order-producer-service
mvn spring-boot:run
```

3. When running locally against the NodePort from `kafka-statefulset-new.yaml`, use `localhost:30092`.
4. Set `KAFKA_BOOTSTRAP_SERVERS` to `kafka-service:9092` inside the cluster.

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS`
- `ORDER_TOPIC`
- `PAYMENT_SUCCESS_TOPIC`
- `PAYMENT_FAILED_TOPIC`

## Notes for students

- The Java services use Spring Boot and Spring Kafka.
- The code contains TODO markers where design and production hardening are required.
- Use the sample manifest in `k8s/java-services.yaml` to deploy the services.
 
