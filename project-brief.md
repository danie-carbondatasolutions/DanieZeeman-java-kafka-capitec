# Event-Driven Order Management Lab

## Overview

This project contains two example microservice stacks: one in .NET and one in Java. Each stack implements an event-driven order pipeline using Kafka topics and separate services.

The pipeline is:

1. `order-producer` publishes `order.placed`
2. `payment-processor` consumes `order.placed` and publishes either `payment.succeeded` or `payment.failed`
3. `inventory-updater` consumes `payment.succeeded` and logs an inventory update

The Kafka cluster is deployed with `kafka-statefulset-new.yaml` on a Rancher Desktop Kubernetes cluster.

## Goals

- Explore Kafka topic design, partitioning, and producer performance
- Understand consumer groups, offset commit strategy, and fault tolerance
- Compare implementation details in .NET and Java
- Create production-grade, event-driven service design patterns

## Topics and events

- `order.placed`
- `payment.succeeded`
- `payment.failed`

## Tasks

1. Review the code stubs in both the .NET and Java folders.
2. Complete incomplete producer and consumer implementations.
3. Add partitioning logic and explain your choice of message key.
4. Tune producer configurations: `acks`, `linger.ms`, `batch.size`, `retries`.
5. Design consumer group names and explain how they handle scaling.
6. Implement fault tolerance for the payment processor and inventory updater.
7. Add idempotency guards or dead-letter handling for duplicate or failed messages.
8. Deploy the services to Rancher Desktop Kubernetes using the included sample manifests.
9. Compare how each platform handles Kafka client configuration, serialization, and error handling.

