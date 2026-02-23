package com.abbank.streams.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP health-check server for container orchestration.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET /health}  — 200 if RUNNING, 503 otherwise</li>
 *   <li>{@code GET /ready}   — 200 if RUNNING or REBALANCING, 503 otherwise</li>
 *   <li>{@code GET /metrics} — Streams state summary as JSON</li>
 * </ul>
 */
public class HealthServer {

    private static final Logger LOG    = LoggerFactory.getLogger(HealthServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaStreams streams;
    private final int          port;
    private       HttpServer   server;

    public HealthServer(final KafkaStreams streams, final int port) {
        this.streams = streams;
        this.port    = port;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/health",  exchange -> handleHealth(exchange));
            server.createContext("/ready",   exchange -> handleReady(exchange));
            server.createContext("/metrics", exchange -> handleMetrics(exchange));
            server.setExecutor(Executors.newSingleThreadExecutor());
            server.start();
            LOG.info("Health server started on port {} (endpoints: /health /ready /metrics)", port);
        } catch (Exception e) {
            LOG.error("Failed to start health server on port {}", port, e);
        }
    }

    public void stop() {
        if (server != null) { server.stop(2); }
    }

    private void handleHealth(final com.sun.net.httpserver.HttpExchange ex) throws java.io.IOException {
        final KafkaStreams.State state = streams.state();
        send(ex, state == KafkaStreams.State.RUNNING ? 200 : 503,
             build(state, "liveness"));
    }

    private void handleReady(final com.sun.net.httpserver.HttpExchange ex) throws java.io.IOException {
        final KafkaStreams.State state = streams.state();
        final boolean ready = state == KafkaStreams.State.RUNNING
                           || state == KafkaStreams.State.REBALANCING;
        send(ex, ready ? 200 : 503, build(state, "readiness"));
    }

    private void handleMetrics(final com.sun.net.httpserver.HttpExchange ex) throws java.io.IOException {
        final Map<String, Object> body = new LinkedHashMap<>();
        body.put("state",       streams.state().toString());
        body.put("application", "cdc-stream-processor");
        body.put("version",     "1.0.0");
        body.put("timestamp",   Instant.now().toString());
        send(ex, 200, body);
    }

    private Map<String, Object> build(final KafkaStreams.State state, final String probe) {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", state.toString()); m.put("probe", probe);
        m.put("timestamp", Instant.now().toString()); return m;
    }

    private void send(final com.sun.net.httpserver.HttpExchange ex,
                      final int status, final Object body) throws java.io.IOException {
        final byte[] bytes = MAPPER.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }
}
