package com.abbank.streams;

import com.abbank.streams.config.ABBankStreamsConfig;
import com.abbank.streams.health.HealthServer;
import com.abbank.streams.topology.ABBankTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the AB Bank Kafka Streams application.
 *
 * <p>Orchestrates five stateful stream-processing pipelines:
 * <ol>
 *   <li>Velocity fraud detection — high-frequency transaction bursts</li>
 *   <li>High-value alert — single transactions above configurable threshold</li>
 *   <li>Running balance reconciliation — per-account rolling balance</li>
 *   <li>Account dormancy detection — idle accounts using session windows</li>
 *   <li>Daily spend aggregation — per-customer daily debit totals</li>
 * </ol>
 *
 * <p>All output is written to typed Kafka topics consumed by the
 * downstream Notification Gateway (email / SMS adapters).
 */
public final class ABBankStreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ABBankStreamsApp.class);

    private ABBankStreamsApp() { }

    public static void main(final String[] args) throws InterruptedException {
        LOG.info("╔══════════════════════════════════════════════════╗");
        LOG.info("║     CDC Stream Processor        v{}              ║", "1.0.0");
        LOG.info("╚══════════════════════════════════════════════════╝");

        final ABBankStreamsConfig config = ABBankStreamsConfig.load();
        final Properties kafkaProps = config.toKafkaProperties();

        final StreamsBuilder builder = new StreamsBuilder();
        ABBankTopology.build(builder, config);
        final Topology topology = builder.build();

        LOG.info("Topology description:\n{}", topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, kafkaProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // ── Uncaught exception handler ────────────────────────────────────
        streams.setUncaughtExceptionHandler((exception) -> {
            LOG.error("Uncaught exception in Streams thread — replacing thread", exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // ── State-change listener ─────────────────────────────────────────
        streams.setStateListener((newState, oldState) -> {
            LOG.info("Streams state change: {} → {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                LOG.error("Streams entered ERROR state — initiating shutdown");
                latch.countDown();
            }
        });

        // ── Shutdown hook ─────────────────────────────────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received — closing streams gracefully");
            streams.close(Duration.ofSeconds(30));
            latch.countDown();
        }, "streams-shutdown-hook"));

        // ── Health check HTTP server ───────────────────────────────────────
        final HealthServer healthServer = new HealthServer(streams, config.getHealthPort());
        healthServer.start();

        try {
            streams.start();
            LOG.info("Streams application started. Awaiting termination signal.");
            latch.await();
        } finally {
            healthServer.stop();
            LOG.info("AB Bank Streams application stopped.");
        }
    }
}
