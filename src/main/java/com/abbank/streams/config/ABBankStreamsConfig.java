package com.abbank.streams.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Centralised configuration loaded from {@code application.conf} (Typesafe Config).
 * All values can be overridden via environment variables or JVM system properties.
 */
public final class ABBankStreamsConfig {

    // ── Input topics (produced by XStream CDC connector) ──────────────────
    public static final String TOPIC_TRANSACTIONS     = "XEPDB1.BANKDB.TRANSACTIONS";
    public static final String TOPIC_ACCOUNTS         = "XEPDB1.BANKDB.ACCOUNTS";
    public static final String TOPIC_CUSTOMERS        = "XEPDB1.BANKDB.CUSTOMERS";

    // ── Output topics (consumed by Notification Gateway) ──────────────────
    public static final String TOPIC_FRAUD_ALERTS       = "abbank.notifications.fraud-alerts";
    public static final String TOPIC_HIGH_VALUE_ALERTS  = "abbank.notifications.high-value-alerts";
    public static final String TOPIC_BALANCE_UPDATES    = "abbank.notifications.balance-updates";
    public static final String TOPIC_DORMANCY_ALERTS    = "abbank.notifications.dormancy-alerts";
    public static final String TOPIC_DAILY_SPEND        = "abbank.notifications.daily-spend";

    // ── State store names ──────────────────────────────────────────────────
    public static final String STORE_TXN_VELOCITY    = "txn-velocity-store";
    public static final String STORE_ACCOUNT_BALANCE = "account-balance-store";
    public static final String STORE_DAILY_SPEND     = "daily-spend-store";
    public static final String STORE_ACCOUNTS_TABLE  = "accounts-table-store";
    public static final String STORE_CUSTOMERS_TABLE = "customers-table-store";

    private final Config raw;

    private ABBankStreamsConfig(final Config config) {
        this.raw = config;
    }

    /** Load from classpath {@code application.conf}, then resolve. */
    public static ABBankStreamsConfig load() {
        return new ABBankStreamsConfig(ConfigFactory.load().resolve());
    }

    public Properties toKafkaProperties() {
        final Properties props = new Properties();

        // Core identity
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                raw.getString("kafka.streams.application-id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                raw.getString("kafka.bootstrap-servers"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG,
                raw.getString("kafka.streams.client-id"));

        // Serialisation
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class.getName());

        // Schema Registry
        props.put("schema.registry.url", raw.getString("kafka.schema-registry-url"));

        // Performance
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                raw.getInt("kafka.streams.num-stream-threads"));
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                raw.getLong("kafka.streams.commit-interval-ms"));
        // STATESTORE_CACHE_MAX_BYTES_CONFIG replaces the removed CACHE_MAX_BYTES_BUFFERING_CONFIG
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG,
                raw.getLong("kafka.streams.cache-max-bytes-buffering"));

        // Reliability
        // replication.factor default is -1 (use broker default) since Kafka 3.0/KIP-733.
        // Explicitly set to -1 so internal changelog topics inherit the broker's configured
        // default.replication.factor rather than being hardcoded to 1.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, -1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.AT_LEAST_ONCE);

        // Exception handlers:
        // DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG deprecated since 4.0
        // → use DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
        // DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG deprecated since 4.0
        // → use PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class);

        return props;
    }

    // ── Schema Registry ───────────────────────────────────────────────────
    /**
     * URL of the Confluent Schema Registry.
     * Used by {@link com.abbank.streams.serde.AvroSerdes} to configure
     * {@link io.confluent.kafka.streams.serdes.avro.GenericAvroSerde}.
     *
     * <p>Subject naming convention used by the XStream CDC connector:
     * <ul>
     *   <li>Keys:   {@code XEPDB1.BANKDB.<TABLE>-key}  (ids: 7, 1, 3, 5)</li>
     *   <li>Values: {@code XEPDB1.BANKDB.<TABLE>-value} (ids: 8, 2, 4, 6)</li>
     * </ul>
     */
    public String getSchemaRegistryUrl() {
        return raw.getString("kafka.schema-registry-url");
    }

    // ── Business thresholds ────────────────────────────────────────────────
    public double getHighValueThreshold() {
        return raw.getDouble("abbank.thresholds.high-value-ngn");
    }

    public int getVelocityMaxTransactions() {
        return raw.getInt("abbank.thresholds.velocity-max-transactions");
    }

    public long getVelocityWindowSeconds() {
        return raw.getLong("abbank.thresholds.velocity-window-seconds");
    }

    public long getDormancyDays() {
        return raw.getLong("abbank.thresholds.dormancy-days");
    }

    public double getDailySpendAlertThreshold() {
        return raw.getDouble("abbank.thresholds.daily-spend-alert-ngn");
    }

    public int getHealthPort() {
        return raw.getInt("abbank.health.port");
    }
}
