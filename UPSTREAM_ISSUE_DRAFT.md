# Quarkus issue draft

**Title:** Quarkus 4: Arc shutdown runs after Vert.x teardown; Kafka consumers do not send LeaveGroup

**Reproducer:** Public repository link to be added after this project is published.

## Environment

- Quarkus `4.0.0.Beta3`
- SmallRye Reactive Messaging `5.0.0-RC2`
- OpenJDK 25
- Redpanda `v26.1.12`

## Description

The reproducer starts a Kafka consumer in `ATest`, waits until its group is
stable, then forces a Quarkus restart before `BTest`. `BTest` publishes a record
and waits 20 seconds for the restarted consumer.

On Beta3, `mvn verify` logs `SRMSG18218` while shutting down the first
application:

```text
RejectedExecutionException: event executor terminated
  at ...KafkaThrottledLatestProcessedCommit...
  at ...KafkaSource.closeQuietly...
  at ...KafkaConnector.terminate...
  at ...Arc.shutdown...
  at ...ServiceGraph.stop...
  at ...ApplicationImpl.doStop...
```

The consumer close also logs:

```text
NullPointerException
  at SmallRyeThreadContext.getCurrentThreadContextOrDefaultContexts
  at ReactiveKafkaConsumer.close
  at KafkaSource.closeQuietly
  at KafkaConnector.terminate
```

The first consumer does not send a `LeaveGroup` request. While `BTest` waits,
`rpk group describe kafka-shutdown-repro` reports `PreparingRebalance` with
2 members, and the message is not received. The members eventually expire
according to `max.poll.interval.ms`.

Expected behavior is for the Kafka consumer to close and send `LeaveGroup`
during application shutdown, allowing the restarted consumer to receive its
partition promptly.

## Controls

- `mvn verify -Dkafka.max.poll.interval.ms=10000` passes after the stale member
  is evicted.
- The same project on Quarkus `4.0.0.Beta1` passes and logs the consumer sending
  a `LeaveGroup` request during the first shutdown.

## Suspected area

The Arc container stop handler (`StaticServiceActions ... initializeContainer`
to `Arc.shutdown`) may not be ordered before the Vert.x and context-propagation
shutdown tasks registered through `VertxCoreRecorder`. The connector's
`@BeforeDestroyed(ApplicationScoped)` termination then runs after those
executors have stopped. Could the Quarkus service stop ordering be reviewed?

Related older reports found during the search: [#3296](https://github.com/quarkusio/quarkus/issues/3296)
and [#47237](https://github.com/quarkusio/quarkus/issues/47237); neither appears
to describe this Quarkus 4 service-graph regression.
