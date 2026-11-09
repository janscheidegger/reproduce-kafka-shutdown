# Kafka consumer shutdown reproducer

Minimal Quarkus project for reproducing Kafka consumer shutdown behavior on Quarkus
4.0.0.Beta3 with SmallRye Reactive Messaging 5.0.0-RC2. It contains one incoming
Kafka channel and no Gluon or business-specific dependencies.

## Requirements

- JDK 25
- Docker

## Reproduce

```shell
./mvnw verify
```

The test profiles force an application restart between `ATest` and `BTest` while
retaining the same public Redpanda container. `ATest` waits until its consumer
has joined the group. `BTest` produces a record and expects the restarted
consumer to receive it within 20 seconds. A shared test resource is used instead
of per-profile Dev Services so the broker survives the forced restart.

The expected reproducer result is that `BTest` times out after the first
application shutdown logs the Kafka consumer close failure. The Kafka group is
`kafka-shutdown-repro`; while `BTest` is waiting, inspect it with:

```shell
docker exec <redpanda-container> rpk group describe kafka-shutdown-repro
```

Use the following control to shorten the stale-member timeout:

```shell
./mvnw verify -Dkafka.max.poll.interval.ms=10000
```

The consumer group ID is not configured explicitly; the application name is used
as the group ID. The shared test resource starts the public Redpanda image and
provides the Kafka Companion.
