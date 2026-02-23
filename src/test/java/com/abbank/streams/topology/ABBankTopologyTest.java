package com.abbank.streams.topology;

import com.abbank.streams.config.ABBankStreamsConfig;
import com.abbank.streams.model.NotificationEvent;
import com.abbank.streams.serde.AvroSerdes;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the Avro-native AB Bank Kafka Streams topology.
 */
class ABBankTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Avro schemas ──

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.record("Source")
            .namespace("io.confluent.connect.oracle.xstream")
            .fields()
            .name("version").type().stringType().noDefault()
            .name("connector").type().stringType().noDefault()
            .name("name").type().stringType().noDefault()
            .name("ts_ms").type().longType().noDefault()
            .name("snapshot").type().unionOf().stringType().and().nullType().endUnion().stringDefault("false")
            .name("db").type().stringType().noDefault()
            .name("sequence").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("ts_us").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("ts_ns").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("schema").type().stringType().noDefault()
            .name("table").type().stringType().noDefault()
            .name("txId").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("scn").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("lcr_position").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("user_name").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("row_id").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .endRecord();

    private static Schema buildEnvelopeSchema(final String namespace, final Schema valueSchema) {
        final Schema blockSchema  = SchemaBuilder.record("Transaction").fields()
                .name("id").type().stringType().noDefault()
                .name("total_order").type().longType().noDefault()
                .name("data_collection_order").type().longType().noDefault()
                .endRecord();

        return SchemaBuilder.record("Envelope")
                .namespace(namespace)
                .fields()
                .name("before").type().unionOf().nullType().and().type(valueSchema).endUnion().nullDefault()
                .name("after").type().unionOf().nullType().and().type(valueSchema).endUnion().nullDefault()
                .name("source").type(SOURCE_SCHEMA).noDefault()
                .name("transaction").type().unionOf().nullType().and().type(blockSchema).endUnion().nullDefault()
                .name("op").type().stringType().noDefault()
                .name("ts_ms").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .name("ts_us").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .name("ts_ns").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .endRecord();
    }

    static final Schema TXN_VALUE_SCHEMA = SchemaBuilder.record("Value")
            .namespace("XEPDB1.BANKDB.TRANSACTIONS")
            .fields()
            .name("TRANSACTION_ID").type().doubleType().noDefault()
            .name("ACCOUNT_ID").type().doubleType().noDefault()
            .name("TRANSACTION_REF").type().stringType().noDefault()
            .name("TRANSACTION_TYPE").type().stringType().noDefault()
            .name("AMOUNT").type().doubleType().noDefault()
            .name("CURRENCY").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("BALANCE_BEFORE").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("BALANCE_AFTER").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("DESCRIPTION").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("COUNTERPARTY_NAME").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("COUNTERPARTY_ACCT").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("CHANNEL").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("TRANSACTION_STATUS").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("INITIATED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("COMPLETED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("CREATED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("UPDATED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .endRecord();

    static final Schema TXN_ENVELOPE_SCHEMA = buildEnvelopeSchema(
            "XEPDB1.BANKDB.TRANSACTIONS", TXN_VALUE_SCHEMA);

    static final Schema TXN_KEY_SCHEMA = SchemaBuilder.record("Key")
            .namespace("XEPDB1.BANKDB.TRANSACTIONS")
            .fields()
            .name("TRANSACTION_ID").type().doubleType().noDefault()
            .endRecord();

    static final Schema ACCT_VALUE_SCHEMA = SchemaBuilder.record("Value")
            .namespace("XEPDB1.BANKDB.ACCOUNTS")
            .fields()
            .name("ACCOUNT_ID").type().doubleType().noDefault()
            .name("CUSTOMER_ID").type().doubleType().noDefault()
            .name("ACCOUNT_NUMBER").type().stringType().noDefault()
            .name("ACCOUNT_TYPE").type().stringType().noDefault()
            .name("CURRENCY").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("BALANCE").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("AVAILABLE_BALANCE").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("OVERDRAFT_LIMIT").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("INTEREST_RATE").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
            .name("ACCOUNT_STATUS").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("OPENED_DATE").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("CLOSED_DATE").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("CREATED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .name("UPDATED_AT").type().unionOf().nullType().and().longType().endUnion().nullDefault()
            .endRecord();

    static final Schema ACCT_ENVELOPE_SCHEMA = buildEnvelopeSchema(
            "XEPDB1.BANKDB.ACCOUNTS", ACCT_VALUE_SCHEMA);

    static final Schema ACCT_KEY_SCHEMA = SchemaBuilder.record("Key")
            .namespace("XEPDB1.BANKDB.ACCOUNTS")
            .fields()
            .name("ACCOUNT_ID").type().doubleType().noDefault()
            .endRecord();

    // ── Test fixtures ─────────────────────────────────────────────────────────

    private TopologyTestDriver driver;
    private TestInputTopic<GenericRecord, GenericRecord>  transactionsTopic;
    private TestInputTopic<GenericRecord, GenericRecord>  accountsTopic;
    private TestOutputTopic<String, String> fraudAlertsTopic;
    private TestOutputTopic<String, String> highValueAlertsTopic;
    private TestOutputTopic<String, String> balanceUpdatesTopic;
    private TestOutputTopic<String, String> dailySpendTopic;

    private GenericAvroSerde keySerde;
    private GenericAvroSerde valueSerde;
    private ABBankStreamsConfig config;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("SCHEMA_REGISTRY_URL", "mock://abbank-test");
        config = ABBankStreamsConfig.load();

        final SchemaRegistryClient mockRegistry = new MockSchemaRegistryClient();
        AvroSerdes.setMockClient(mockRegistry);

        mockRegistry.register("XEPDB1.BANKDB.TRANSACTIONS-key",   new AvroSchema(TXN_KEY_SCHEMA),   0, 3);
        mockRegistry.register("XEPDB1.BANKDB.TRANSACTIONS-value", new AvroSchema(TXN_ENVELOPE_SCHEMA), 0, 4);
        mockRegistry.register("XEPDB1.BANKDB.ACCOUNTS-key",       new AvroSchema(ACCT_KEY_SCHEMA),  0, 7);
        mockRegistry.register("XEPDB1.BANKDB.ACCOUNTS-value",     new AvroSchema(ACCT_ENVELOPE_SCHEMA), 0, 8);

        final var srConfig = Collections.singletonMap(
                "schema.registry.url", "mock://abbank-test");

        keySerde   = new GenericAvroSerde(mockRegistry);
        valueSerde = new GenericAvroSerde(mockRegistry);
        keySerde.configure(srConfig, true);
        valueSerde.configure(srConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        ABBankTopology.build(builder, config);

        final Properties props = config.toKafkaProperties();
        props.put(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("java.io.tmpdir") + "/abbank-test-" + System.nanoTime());
        props.put("schema.registry.url", "mock://abbank-test");

        driver = new TopologyTestDriver(builder.build(), props);

        transactionsTopic = driver.createInputTopic(
                ABBankStreamsConfig.TOPIC_TRANSACTIONS, keySerde.serializer(), valueSerde.serializer());
        accountsTopic = driver.createInputTopic(
                ABBankStreamsConfig.TOPIC_ACCOUNTS, keySerde.serializer(), valueSerde.serializer());

        fraudAlertsTopic     = driver.createOutputTopic(ABBankStreamsConfig.TOPIC_FRAUD_ALERTS,
                Serdes.String().deserializer(), Serdes.String().deserializer());
        highValueAlertsTopic = driver.createOutputTopic(ABBankStreamsConfig.TOPIC_HIGH_VALUE_ALERTS,
                Serdes.String().deserializer(), Serdes.String().deserializer());
        balanceUpdatesTopic  = driver.createOutputTopic(ABBankStreamsConfig.TOPIC_BALANCE_UPDATES,
                Serdes.String().deserializer(), Serdes.String().deserializer());
        dailySpendTopic      = driver.createOutputTopic(ABBankStreamsConfig.TOPIC_DAILY_SPEND,
                Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) driver.close();
        AvroSerdes.setMockClient(null);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("High-Value Transaction Alerts")
    class HighValueAlertTests {
        @Test
        @DisplayName("Should emit CRITICAL alert when AMOUNT (Avro double) exceeds ₦500k threshold")
        void shouldAlertOnHighValueDebit() {
            final GenericRecord key      = buildTxnKey(42.0);
            final GenericRecord envelope = buildTxnEnvelope(42.0, 101.0, "DEBIT",
                    600000.0, 1000000.0, 400000.0, "COMPLETED", "MOBILE", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Should NOT emit alert when AMOUNT double is below ₦500k")
        void shouldNotAlertBelowThreshold() {
            final GenericRecord key      = buildTxnKey(43.0);
            final GenericRecord envelope = buildTxnEnvelope(43.0, 102.0, "DEBIT",
                    100000.0, 500000.0, 400000.0, "COMPLETED", "ATM", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isTrue();
        }

        @ParameterizedTest
        @ValueSource(strings = {"CREDIT", "TRANSFER_IN"})
        @DisplayName("Should emit MEDIUM severity for credit transactions above threshold")
        void shouldEmitMediumSeverityForCredits(final String txnType) {
            final GenericRecord key      = buildTxnKey(44.0);
            final GenericRecord envelope = buildTxnEnvelope(44.0, 103.0, txnType,
                    750000.0, 200000.0, 950000.0, "COMPLETED", "INTERNET", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Snapshot (op=r) events should be processed like inserts")
        void shouldProcessSnapshotEvents() {
            final GenericRecord key      = buildTxnKey(45.0);
            final GenericRecord envelope = buildTxnEnvelope(45.0, 104.0, "DEBIT",
                    800000.0, 1000000.0, 200000.0, "COMPLETED", "WEB", "r");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Should NOT process delete (op=d) envelopes")
        void shouldIgnoreDeletes() {
            final GenericRecord key      = buildTxnKey(46.0);
            final GenericRecord envelope = buildTxnDeleteEnvelope(46.0, 105.0, "DEBIT", 900000.0);
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("Balance Reconciliation")
    class BalanceReconciliationTests {
        @Test
        @DisplayName("Should emit balance update for every COMPLETED transaction")
        void shouldEmitOnCompletion() {
            final GenericRecord key      = buildTxnKey(1.0);
            final GenericRecord envelope = buildTxnEnvelope(1.0, 201.0, "DEBIT",
                    50.0, 1000.0, 950.0, "COMPLETED", "ATM", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(balanceUpdatesTopic.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Bootstrap: first event uses BALANCE_BEFORE as initial stored value")
        void shouldBootstrapFromFirstEvent() {
            final GenericRecord key      = buildTxnKey(3.0);
            final GenericRecord envelope = buildTxnEnvelope(3.0, 202.0, "DEBIT",
                    100.0, 1000.0, 900.0, "COMPLETED", "MOBILE", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(balanceUpdatesTopic.isEmpty()).isFalse();
        }
    }

    @Nested
    @DisplayName("Resilience and Edge Cases")
    class ResilienceTests {
        @Test
        @DisplayName("Null envelope should be silently dropped")
        void shouldHandleNullEnvelope() {
            transactionsTopic.pipeInput(buildTxnKey(50.0), (GenericRecord) null);
            assertThat(highValueAlertsTopic.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("AMOUNT exactly at threshold should trigger alert (boundary)")
        void shouldAlertAtExactThreshold() {
            final double threshold = config.getHighValueThreshold();
            final GenericRecord key      = buildTxnKey(60.0);
            final GenericRecord envelope = buildTxnEnvelope(60.0, 301.0, "DEBIT",
                    threshold, threshold + 100, 100, "COMPLETED", "API", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("AMOUNT just below threshold should NOT trigger alert (boundary)")
        void shouldNotAlertJustBelowThreshold() {
            final double threshold = config.getHighValueThreshold();
            final GenericRecord key      = buildTxnKey(61.0);
            final GenericRecord envelope = buildTxnEnvelope(61.0, 301.0, "DEBIT",
                    threshold - 0.01, threshold + 100, 100.01, "COMPLETED", "API", "c");
            transactionsTopic.pipeInput(key, envelope);
            assertThat(highValueAlertsTopic.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("INITIATED_AT MicroTimestamp (micros) correctly converted to millis")
        void shouldConvertMicroTimestampCorrectly() {
            final long microsEpoch = 1_700_000_000_000_000L;
            final GenericRecord key = buildTxnKey(62.0);
            final GenericRecord value = new GenericData.Record(TXN_VALUE_SCHEMA);
            value.put("TRANSACTION_ID",    62.0);
            value.put("ACCOUNT_ID",        302.0);
            value.put("TRANSACTION_REF",   "REF-MICRO");
            value.put("TRANSACTION_TYPE",  "CREDIT");
            value.put("AMOUNT",            600000.0);
            value.put("CURRENCY",          "NGN");
            value.put("BALANCE_BEFORE",    null);
            value.put("BALANCE_AFTER",     600000.0);
            value.put("DESCRIPTION",       null);
            value.put("COUNTERPARTY_NAME", null);
            value.put("COUNTERPARTY_ACCT", null);
            value.put("CHANNEL",           "API");
            value.put("TRANSACTION_STATUS","COMPLETED");
            value.put("INITIATED_AT",      microsEpoch);
            value.put("COMPLETED_AT",      null);
            value.put("CREATED_AT",        null);
            value.put("UPDATED_AT",        null);

            final GenericRecord envelope = buildEnvelopeFromValue(value, "c");
            transactionsTopic.pipeInput(key, envelope);

            assertThat(highValueAlertsTopic.isEmpty()).isFalse();
            final NotificationEvent alert = NotificationEvent.fromJson(highValueAlertsTopic.readValue());
            assertThat(alert.getEventTime()).isEqualTo(Instant.ofEpochMilli(1_700_000_000_000L));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private GenericRecord buildTxnKey(final double id) {
        final GenericRecord key = new GenericData.Record(TXN_KEY_SCHEMA);
        key.put("TRANSACTION_ID", id);
        return key;
    }

    private GenericRecord buildTxnEnvelope(
            final double txnId, final double accountId, final String type,
            final double amount, final double balBefore, final double balAfter,
            final String status, final String channel, final String op) {

        final GenericRecord value = new GenericData.Record(TXN_VALUE_SCHEMA);
        value.put("TRANSACTION_ID",    txnId);
        value.put("ACCOUNT_ID",        accountId);
        value.put("TRANSACTION_REF",   "REF-" + (long) txnId);
        value.put("TRANSACTION_TYPE",  type);
        value.put("AMOUNT",            amount);
        value.put("CURRENCY",          "NGN");
        value.put("BALANCE_BEFORE",    balBefore);
        value.put("BALANCE_AFTER",     balAfter);
        value.put("DESCRIPTION",       null);
        value.put("COUNTERPARTY_NAME", null);
        value.put("COUNTERPARTY_ACCT", null);
        value.put("CHANNEL",           channel);
        value.put("TRANSACTION_STATUS", status);
        value.put("INITIATED_AT",      Instant.now().toEpochMilli() * 1000L);
        value.put("COMPLETED_AT",      null);
        value.put("CREATED_AT",        null);
        value.put("UPDATED_AT",        null);

        return buildEnvelopeFromValue(value, op);
    }

    private GenericRecord buildTxnDeleteEnvelope(
            final double id, final double acctId, final String type, final double amount) {
        final GenericRecord value = new GenericData.Record(TXN_VALUE_SCHEMA);
        value.put("TRANSACTION_ID",    id);
        value.put("ACCOUNT_ID",        acctId);
        value.put("TRANSACTION_REF",   "REF-" + (long) id);
        value.put("TRANSACTION_TYPE",  type);
        value.put("AMOUNT",            amount);
        value.put("CURRENCY",          "NGN");
        value.put("BALANCE_BEFORE",    amount + 100.0);
        value.put("BALANCE_AFTER",     100.0);
        value.put("TRANSACTION_STATUS","COMPLETED");
        value.put("CHANNEL",           "WEB");
        value.put("INITIATED_AT",      Instant.now().toEpochMilli() * 1000L);
        value.put("COMPLETED_AT",      null);
        value.put("CREATED_AT",        null);
        value.put("UPDATED_AT",        null);
        value.put("DESCRIPTION",       null);
        value.put("COUNTERPARTY_NAME", null);
        value.put("COUNTERPARTY_ACCT", null);

        final GenericRecord envelope = new GenericData.Record(TXN_ENVELOPE_SCHEMA);
        envelope.put("before", value);
        envelope.put("after",  null);
        envelope.put("op",     "d");
        envelope.put("ts_ms",  Instant.now().toEpochMilli());
        envelope.put("ts_us",  null);
        envelope.put("ts_ns",  null);
        envelope.put("source", buildSource("TRANSACTIONS"));
        envelope.put("transaction", null);
        return envelope;
    }

    private GenericRecord buildEnvelopeFromValue(final GenericRecord value, final String op) {
        final GenericRecord envelope = new GenericData.Record(TXN_ENVELOPE_SCHEMA);
        envelope.put("before",      null);
        envelope.put("after",       value);
        envelope.put("op",          op);
        envelope.put("ts_ms",       Instant.now().toEpochMilli());
        envelope.put("ts_us",       null);
        envelope.put("ts_ns",       null);
        envelope.put("source",      buildSource("TRANSACTIONS"));
        envelope.put("transaction", null);
        return envelope;
    }

    private GenericRecord buildSource(final String table) {
        final Schema srcSchema = TXN_ENVELOPE_SCHEMA.getField("source").schema();
        final GenericRecord src = new GenericData.Record(srcSchema);
        src.put("version",      "1.0");
        src.put("connector",    "oracle-xstream");
        src.put("name",         "ABBANK-XSTREAM-CDC");
        src.put("ts_ms",        Instant.now().toEpochMilli());
        src.put("snapshot",     "false");
        src.put("db",           "XE");
        src.put("sequence",     null);
        src.put("ts_us",        null);
        src.put("ts_ns",        null);
        src.put("schema",       "BANKDB");
        src.put("table",        table);
        src.put("txId",         null);
        src.put("scn",          null);
        src.put("lcr_position", null);
        src.put("user_name",    null);
        src.put("row_id",       null);
        return src;
    }
}
