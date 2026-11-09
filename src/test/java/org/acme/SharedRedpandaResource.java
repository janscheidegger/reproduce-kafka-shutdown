package org.acme;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SharedRedpandaResource implements QuarkusTestResourceLifecycleManager {

    // Test-resource instances are reloaded for custom profiles; the fork-wide property keeps their broker address shared.
    private static final String BOOTSTRAP_SERVERS_PROPERTY = "kafka-shutdown-repro.bootstrap-servers";
    private static GenericContainer<?> redpanda;
    private static KafkaCompanion companion;
    private static Map<String, String> config;

    @Override
    public synchronized Map<String, String> start() {
        String bootstrapServers = System.getProperty(BOOTSTRAP_SERVERS_PROPERTY);
        if (bootstrapServers == null) {
            int port = findAvailablePort();
            bootstrapServers = "127.0.0.1:" + port;
            redpanda = new PortBoundRedpanda(port)
                    .withCommand(
                            "redpanda", "start",
                            "--mode", "dev-container",
                            "--smp", "1",
                            "--memory", "1G",
                            "--reserve-memory", "0M",
                            "--overprovisioned",
                            "--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
                            "--advertise-kafka-addr",
                            "internal://localhost:9092,external://127.0.0.1:" + port)
                    .waitingFor(Wait.forListeningPort());
            redpanda.start();
            System.setProperty(BOOTSTRAP_SERVERS_PROPERTY, bootstrapServers);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (companion != null) {
                    companion.close();
                }
                if (redpanda != null) {
                    redpanda.stop();
                }
            }, "redpanda-test-resource-shutdown"));
        }
        companion = new KafkaCompanion(bootstrapServers);
        config = Map.of("kafka.bootstrap.servers", bootstrapServers);
        return config;
    }

    @Override
    public void stop() {
        // Quarkus invokes stop during profile changes; keep the broker until the fork shuts down.
        if (companion != null) {
            companion.close();
        }
    }

    @Override
    public void inject(TestInjector injector) {
        injector.injectIntoFields(companion,
                new TestInjector.AnnotatedAndMatchesType(InjectKafkaCompanion.class, KafkaCompanion.class));
    }

    private static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not find a free Kafka port", e);
        }
    }

    private static class PortBoundRedpanda extends GenericContainer<PortBoundRedpanda> {

        private final int port;

        private PortBoundRedpanda(int port) {
            super(DockerImageName.parse("docker.io/redpandadata/redpanda:v26.1.12"));
            this.port = port;
        }

        @Override
        protected void configure() {
            super.configure();
            addFixedExposedPort(port, 19092);
        }
    }
}
