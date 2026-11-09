package org.acme;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.kafka.common.ConsumerGroupState;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

@QuarkusTest
@TestProfile(ATestProfile.class)
@QuarkusTestResource(SharedRedpandaResource.class)
class ATest {

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Test
    void startsAndJoinsConsumerGroup() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            var group = companion.consumerGroups().describe("kafka-shutdown-repro");
            assertEquals(ConsumerGroupState.STABLE, group.state());
            assertEquals(1, group.members().size());
        });
    }
}
