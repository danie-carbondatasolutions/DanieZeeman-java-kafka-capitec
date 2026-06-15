# .NET Order Management Microservices

## Services

- `OrderProducer`: publishes `order.placed` events
- `PaymentsProcessor`: consumes `order.placed` and publishes `payment.succeeded` or `payment.failed`
- `InventoryUpdater`: consumes `payment.succeeded` and logs inventory updates

## Running locally

1. Deploy Kafka with `kafka-statefulset-new.yaml` in your Rancher Desktop cluster.
2. Build and run each service using `dotnet run` from its folder.
3. Set `KAFKA_BOOTSTRAP_SERVERS` to `kafka-service:9092` inside the cluster or use the NodePort host if running outside.

Example:

```bash
cd dotnet-order-service/OrderProducer
dotnet run
```

## Configuration

The services use environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `ORDER_TOPIC`
- `PAYMENT_SUCCESS_TOPIC`
- `PAYMENT_FAILED_TOPIC`

## Notes

- The current code is intentionally light on production-grade retry, partition, and fault-tolerance logic.
- Add comments and improve error handling before deploying to a cluster.
- Use the sample manifest in `k8s/dotnet-services.yaml` as a starting point.
