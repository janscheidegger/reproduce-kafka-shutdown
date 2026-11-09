package org.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

@QuarkusTest
@TestProfile(BTestProfile.class)
@QuarkusTestResource(SharedRedpandaResource.class)
class BTest {

    @Inject
    MessageCollector collector;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Test
    void receivesRecordAfterRestart() throws InterruptedException {
        companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>("in", "hello"));

        assertEquals("hello", collector.poll(20, TimeUnit.SECONDS));
    }
}
