package com.abbank.streams.topology;

import com.abbank.streams.config.ABBankStreamsConfig;
import com.abbank.streams.model.*;
import com.abbank.streams.serde.AvroSerdes;
import com.abbank.streams.serde.JsonSerde;
import com.abbank.streams.util.CdcParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

/**
 * AB Bank Kafka Streams topology — Avro-native edition.
 *
 * <p>Consumes Avro-encoded CDC envelopes published by the Confluent XStream CDC connector.
 * Each message is a {@link GenericRecord} conforming to the schemas registered in
 * Confluent Schema Registry (see {@code src/main/avro/*.avsc}).
 *
 * <h2>Schema binding key facts</h2>
 * <ul>
 *   <li>All Kafka keys are {@code GenericRecord} with a single {@code double} field
 *       (e.g. {@code TRANSACTION_ID}, {@code ACCOUNT_ID}). Keys are Oracle NUMBER
 *       columns mapped to Avro double by the connector.</li>
 *   <li>All Kafka values are CDC Envelope {@code GenericRecord} with fields:
 *       {@code before}, {@code after} (both nullable Value records),
 *       {@code op} (string), {@code ts_ms} (nullable long), plus {@code source}.</li>
 *   <li>Timestamp fields in Value records use {@code io.debezium.time.MicroTimestamp}
 *       (epoch <em>microseconds</em> as long) — divided by 1000 to get millis.</li>
 *   <li>AMOUNT, BALANCE_BEFORE, BALANCE_AFTER are Avro {@code double}.</li>
 * </ul>
 *
 * <h2>Five processing pipelines</h2>
 * <ol>
 *   <li>Velocity fraud detection — tumbling window count of debits per account</li>
 *   <li>High-value alert — stateless filter on AMOUNT field</li>
 *   <li>Balance reconciliation — persistent RocksDB KV store, discrepancy detection</li>
 *   <li>Dormancy detection — session window on inactivity gap</li>
 *   <li>Daily spend aggregation — tumbling 24h window sum of debits</li>
 * </ol>
 */
public final class ABBankTopology {

    private static final Logger LOG = LoggerFactory.getLogger(ABBankTopology.class);
    private static final ZoneId LAGOS = ZoneId.of("Africa/Lagos");

    private ABBankTopology() { }

    public static void build(final StreamsBuilder builder, final ABBankStreamsConfig config) {
        LOG.info("Building AB Bank Avro topology with Schema Registry at {}",
                config.getSchemaRegistryUrl());

        // ── Register persistent state stores ─────────────────────────────
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ABBankStreamsConfig.STORE_ACCOUNT_BALANCE),
                        Serdes.String(), Serdes.String())
                        .withLoggingEnabled(Collections.emptyMap()));

        // ── Avro serdes backed by Schema Registry ─────────────────────────
        final var keySerde      = AvroSerdes.keySerde(config.getSchemaRegistryUrl());
        final var envelopeSerde = AvroSerdes.envelopeSerde(config.getSchemaRegistryUrl());

        // ── Source: TRANSACTIONS CDC topic ────────────────────────────────
        // Key   = GenericRecord { TRANSACTION_ID: double }   (schema id=3)
        // Value = GenericRecord CDC Envelope                 (schema id=4)
        final KStream<GenericRecord, GenericRecord> rawTransactions = builder.stream(
                ABBankStreamsConfig.TOPIC_TRANSACTIONS,
                Consumed.with(keySerde, envelopeSerde)
                        .withName("source-transactions-avro"));

        // ── Parse envelope → TransactionEvent, drop deletes and nulls ─────
        final KStream<String, TransactionEvent> transactions = rawTransactions
                .filter((key, envelope) -> envelope != null,
                        Named.as("filter-non-null-envelopes"))
                .filter((key, envelope) -> {
                    final String op = strField(envelope, "op");
                    return !"d".equals(op); // drop tombstones/deletes
                }, Named.as("filter-drop-deletes"))
                .mapValues((key, envelope) -> {
                    final GenericRecord row = (GenericRecord) envelope.get("after");
                    if (row == null) return null;
                    return TransactionEvent.fromAvro(row);
                }, Named.as("map-envelope-to-transaction"))
                .filter((key, txn) -> txn != null && txn.getAccountId() != 0L,
                        Named.as("filter-valid-transactions"))
                // Re-key by ACCOUNT_ID (string) for downstream groupByKey
                .selectKey((key, txn) -> String.valueOf(txn.getAccountId()),
                        Named.as("rekey-by-account-id"));

        // ── Accounts KTable for enrichment ────────────────────────────────
        // Key   = GenericRecord { ACCOUNT_ID: double }  (schema id=7)
        // Value = GenericRecord CDC Envelope             (schema id=8)
        final KTable<String, AccountEvent> accountsTable = builder
                .stream(ABBankStreamsConfig.TOPIC_ACCOUNTS,
                        Consumed.with(AvroSerdes.keySerde(config.getSchemaRegistryUrl()),
                                      AvroSerdes.envelopeSerde(config.getSchemaRegistryUrl()))
                                .withName("source-accounts-avro"))
                .filter((key, env) -> env != null && !"d".equals(strField(env, "op")),
                        Named.as("accounts-filter-upserts"))
                .mapValues(env -> AccountEvent.fromAvro((GenericRecord) env.get("after")),
                        Named.as("accounts-map-to-event"))
                .filter((key, acc) -> acc != null, Named.as("accounts-filter-non-null"))
                .selectKey((key, acc) -> String.valueOf(acc.getAccountId()),
                        Named.as("accounts-rekey"))
                .toTable(Named.as("accounts-to-table"),
                        Materialized.<String, AccountEvent>as(
                                Stores.inMemoryKeyValueStore(ABBankStreamsConfig.STORE_ACCOUNTS_TABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(AccountEvent.class)));

        // ── Customers KTable for contact info enrichment ──────────────────
        // Key   = GenericRecord { CUSTOMER_ID: double }  (schema id=1)
        // Value = GenericRecord CDC Envelope              (schema id=2)
        final KTable<String, CustomerEvent> customersTable = builder
                .stream(ABBankStreamsConfig.TOPIC_CUSTOMERS,
                        Consumed.with(AvroSerdes.keySerde(config.getSchemaRegistryUrl()),
                                      AvroSerdes.envelopeSerde(config.getSchemaRegistryUrl()))
                                .withName("source-customers-avro"))
                .filter((key, env) -> env != null && !"d".equals(strField(env, "op")),
                        Named.as("customers-filter-upserts"))
                .mapValues(env -> CustomerEvent.fromAvro((GenericRecord) env.get("after")),
                        Named.as("customers-map-to-event"))
                .filter((key, cust) -> cust != null, Named.as("customers-filter-non-null"))
                .selectKey((key, cust) -> String.valueOf(cust.getCustomerId()),
                        Named.as("customers-rekey"))
                .toTable(Named.as("customers-to-table"),
                        Materialized.<String, CustomerEvent>as(
                                Stores.inMemoryKeyValueStore(ABBankStreamsConfig.STORE_CUSTOMERS_TABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(CustomerEvent.class)));

        LOG.info("Avro source streams and KTables established.");

        // ── Run all five pipelines ─────────────────────────────────────────
        buildVelocityFraudPipeline(transactions, config);
        buildHighValueAlertPipeline(transactions, accountsTable, customersTable, config);
        buildBalanceReconciliationPipeline(transactions, accountsTable, config);
        buildDormancyDetectionPipeline(transactions, config);
        buildDailySpendPipeline(transactions, config);

        LOG.info("AB Bank Avro topology fully built — 5 pipelines active.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline 1: Velocity Fraud Detection
    // Tumbling 60-second window count of completed debits per account.
    // Alerts at >= N transactions within the window.
    // ─────────────────────────────────────────────────────────────────────────
    private static void buildVelocityFraudPipeline(
            final KStream<String, TransactionEvent> transactions,
            final ABBankStreamsConfig config) {

        final long windowSec = config.getVelocityWindowSeconds();
        final int  maxTxns   = config.getVelocityMaxTransactions();

        transactions
            .filter((acctId, txn) -> txn.isDebit() && txn.isCompleted(),
                    Named.as("velocity-filter-debits"))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class))
                    .withName("velocity-group"))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(windowSec)))
            .count(Named.as("velocity-count"),
                   Materialized.as(ABBankStreamsConfig.STORE_TXN_VELOCITY))
            .toStream(Named.as("velocity-stream"))
            .filter((wk, count) -> count != null && count >= maxTxns,
                    Named.as("velocity-threshold"))
            .mapValues((wk, count) -> {
                final String accountId  = wk.key();
                final String windowStart = Instant.ofEpochMilli(wk.window().start())
                        .atZone(LAGOS).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                LOG.warn("FRAUD VELOCITY: account={} count={} in {}s window@{}",
                        accountId, count, windowSec, windowStart);
                return NotificationEvent.builder()
                        .notificationType(NotificationEvent.NotificationType.FRAUD_ALERT)
                        .severity(NotificationEvent.Severity.CRITICAL)
                        .channel(NotificationEvent.Channel.BOTH)
                        .accountId(parseLong(accountId))
                        .subject("🚨 Suspicious Activity Detected on Your Account")
                        .body(String.format(
                            "We detected %d transactions on your account within %d seconds "
                          + "starting at %s.\n"
                          + "This activity has been flagged for fraud review.\n"
                          + "If you did not initiate these, contact AB Bank immediately: 0800-AB-BANK.",
                            count, windowSec, windowStart))
                        .eventTime(Instant.ofEpochMilli(wk.window().start()))
                        .meta("windowStartMs",    wk.window().start())
                        .meta("windowEndMs",      wk.window().end())
                        .meta("transactionCount", count)
                        .meta("windowSizeSec",    windowSec)
                        .build().toJson();
            }, Named.as("velocity-build-notification"))
            .to(ABBankStreamsConfig.TOPIC_FRAUD_ALERTS);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline 2: High-Value Transaction Alert
    // Filters on the Avro AMOUNT double field (≥ threshold).
    // Joins with accountsTable to include account number and customer ID,
    // then joins with customersTable to include name and contact info.
    // ─────────────────────────────────────────────────────────────────────────
    private static void buildHighValueAlertPipeline(
            final KStream<String, TransactionEvent> transactions,
            final KTable<String, AccountEvent> accountsTable,
            final KTable<String, CustomerEvent> customersTable,
            final ABBankStreamsConfig config) {

        final double threshold = config.getHighValueThreshold();

        // Step 1: filter high-value transactions
        // AMOUNT is Avro double — compare directly
        final KStream<String, TransactionEvent> highValue = transactions
                .filter((acctId, txn) ->
                        txn.getAmount() != null
                        && txn.getAmount().doubleValue() >= threshold,
                        Named.as("hv-filter-amount"));

        // Step 2: join with accountsTable to resolve accountNumber + customerId
        // Key is accountId (string) — accounts table is also keyed by accountId
        final KStream<String, String> enriched = highValue
                .leftJoin(accountsTable,
                        (txn, account) -> buildHighValueNotification(txn, account, threshold),
                        Joined.with(Serdes.String(),
                                    new JsonSerde<>(TransactionEvent.class),
                                    new JsonSerde<>(AccountEvent.class))
                              .withName("hv-join-accounts"))
                .filter((k, v) -> v != null, Named.as("hv-filter-null-join"));

        enriched.to(ABBankStreamsConfig.TOPIC_HIGH_VALUE_ALERTS,
                Produced.with(Serdes.String(), Serdes.String())
                        .withName("hv-sink"));
    }

    private static String buildHighValueNotification(
            final TransactionEvent txn,
            final AccountEvent account,
            final double threshold) {

        final String accountNumber = account != null ? account.getAccountNumber() : "N/A";
        final Long   customerId    = account != null ? account.getCustomerId() : null;
        final boolean isDebit      = txn.isDebit();

        LOG.info("HIGH VALUE: acct={} number={} customer={} amount={} type={}",
                txn.getAccountId(), accountNumber, customerId,
                txn.getAmount(), txn.getTransactionType());

        return NotificationEvent.builder()
                .notificationType(NotificationEvent.NotificationType.HIGH_VALUE_ALERT)
                .severity(isDebit ? NotificationEvent.Severity.HIGH : NotificationEvent.Severity.MEDIUM)
                .channel(NotificationEvent.Channel.BOTH)
                .accountId(txn.getAccountId())
                .customerId(customerId)
                .accountNumber(accountNumber)
                .subject(String.format("💰 High-Value %s: %s",
                        isDebit ? "DEBIT" : "CREDIT",
                        CdcParser.formatAmount(txn.getAmount(), txn.getCurrency())))
                .body(String.format(
                    "A high-value %s of %s has been processed on account %s.\n"
                  + "Reference:      %s\n"
                  + "Channel:        %s\n"
                  + "Balance before: %s\n"
                  + "Balance after:  %s\n"
                  + "Time:           %s\n\n"
                  + "Not you? Call 0800-AB-BANK immediately.",
                    isDebit ? "debit" : "credit",
                    CdcParser.formatAmount(txn.getAmount(), txn.getCurrency()),
                    accountNumber,
                    txn.getTransactionRef(),
                    txn.getChannel(),
                    CdcParser.formatAmount(txn.getBalanceBefore(), txn.getCurrency()),
                    CdcParser.formatAmount(txn.getBalanceAfter(), txn.getCurrency()),
                    CdcParser.formatTimestampMs(txn.getInitiatedAtMs())))
                .eventTime(txn.initiatedAt())
                .meta("transactionRef",  txn.getTransactionRef())
                .meta("transactionType", txn.getTransactionType())
                .meta("amountNgn",       txn.getAmount().doubleValue())
                .meta("channel",         txn.getChannel())
                .meta("thresholdNgn",    threshold)
                .meta("customerId",      customerId)
                .build().toJson();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline 3: Balance Reconciliation
    // Stateful transformer with persistent RocksDB store.
    // Reads BALANCE_AFTER (Avro double) from the CDC event and compares
    // against the stored value to detect unexpected discrepancies.
    // ─────────────────────────────────────────────────────────────────────────
    private static void buildBalanceReconciliationPipeline(
            final KStream<String, TransactionEvent> transactions,
            final KTable<String, AccountEvent> accountsTable,
            final ABBankStreamsConfig config) {

        // KStream.processValues() is the Kafka Streams 3.x replacement for the
        // deprecated transformValues(). It accepts a FixedKeyProcessorSupplier and
        // an optional Named + state store names.
        transactions
            .filter((acctId, txn) -> txn.isCompleted() && txn.getBalanceAfter() != null
                    && txn.getBalanceAfter().compareTo(BigDecimal.ZERO) >= 0,
                    Named.as("balance-filter-completed"))
            .processValues(
                BalanceReconciliationProcessor::new,
                Named.as("balance-reconcile"),
                ABBankStreamsConfig.STORE_ACCOUNT_BALANCE)
            .filter((k, v) -> v != null, Named.as("balance-filter-output"))
            .to(ABBankStreamsConfig.TOPIC_BALANCE_UPDATES,
                    Produced.with(Serdes.String(), Serdes.String())
                            .withName("balance-sink"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline 4: Dormancy Detection (Session Window)
    // Session closes after configurable inactivity gap.
    // Accounts with exactly 1 event in a session = no follow-up activity.
    // ─────────────────────────────────────────────────────────────────────────
    private static void buildDormancyDetectionPipeline(
            final KStream<String, TransactionEvent> transactions,
            final ABBankStreamsConfig config) {

        final Duration inactivityGap = Duration.ofDays(config.getDormancyDays());

        transactions
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class))
                    .withName("dormancy-group"))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap))
            .count(Named.as("dormancy-count"))
            .toStream(Named.as("dormancy-stream"))
            .filter((wk, count) -> count != null && count == 1L,
                    Named.as("dormancy-single-event"))
            .mapValues((wk, count) -> {
                final String accountId = wk.key();
                LOG.info("DORMANCY: account={} last-active={}", accountId,
                        CdcParser.formatTimestampMs(wk.window().start()));
                return NotificationEvent.builder()
                        .notificationType(NotificationEvent.NotificationType.DORMANCY_ALERT)
                        .severity(NotificationEvent.Severity.LOW)
                        .channel(NotificationEvent.Channel.EMAIL)
                        .accountId(parseLong(accountId))
                        .subject("⏰ Your AB Bank Account Has Been Inactive")
                        .body(String.format(
                            "Dear Valued Customer,\n\n"
                          + "Your AB Bank account (ID: %s) has had no transaction activity "
                          + "for %d days.\n\n"
                          + "To keep your account active and avoid dormancy charges, "
                          + "please make at least one transaction.\n\n"
                          + "Log in to the AB Bank app or visit any branch.\n\n"
                          + "Regards,\nAB Bank Customer Experience",
                            accountId, config.getDormancyDays()))
                        .eventTime(Instant.ofEpochMilli(wk.window().start()))
                        .meta("dormancyDays",  config.getDormancyDays())
                        .meta("sessionStart",  wk.window().start())
                        .meta("sessionEnd",    wk.window().end())
                        .build().toJson();
            }, Named.as("dormancy-build-notification"))
            .to(ABBankStreamsConfig.TOPIC_DORMANCY_ALERTS);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline 5: Daily Spend Aggregation (Tumbling 24h Window)
    // Sums AMOUNT (Avro double) for debit transactions per account per day.
    // AMOUNT is directly a double in the schema — no string parsing needed.
    // ─────────────────────────────────────────────────────────────────────────
    private static void buildDailySpendPipeline(
            final KStream<String, TransactionEvent> transactions,
            final ABBankStreamsConfig config) {

        final double alertThreshold = config.getDailySpendAlertThreshold();

        transactions
            .filter((acctId, txn) -> txn.isDebit() && txn.isCompleted(),
                    Named.as("daily-filter-debits"))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class))
                    .withName("daily-group"))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
            .aggregate(
                () -> BigDecimal.ZERO,
                // AMOUNT is Avro double, already converted to BigDecimal in TransactionEvent
                (acctId, txn, running) -> running.add(
                        txn.getAmount() != null ? txn.getAmount() : BigDecimal.ZERO),
                Named.as("daily-spend-agg"),
                Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(ABBankStreamsConfig.STORE_DAILY_SPEND)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(BigDecimal.class)))
            .toStream(Named.as("daily-stream"))
            .filter((wk, total) ->
                    total != null && total.doubleValue() >= alertThreshold,
                    Named.as("daily-threshold"))
            .mapValues((wk, total) -> {
                final String accountId = wk.key();
                final String date      = Instant.ofEpochMilli(wk.window().start())
                        .atZone(LAGOS).toLocalDate().toString();
                LOG.info("DAILY SPEND: account={} date={} total={}", accountId, date, total);
                return NotificationEvent.builder()
                        .notificationType(NotificationEvent.NotificationType.DAILY_SPEND_SUMMARY)
                        .severity(NotificationEvent.Severity.MEDIUM)
                        .channel(NotificationEvent.Channel.SMS)
                        .accountId(parseLong(accountId))
                        .subject(String.format("📊 Daily Spend Alert — %s", date))
                        .body(String.format(
                            "AB Bank: Your total debits today (%s) reached %s, "
                          + "exceeding your alert threshold of %s. "
                          + "Reply STOP to unsubscribe.",
                            date,
                            CdcParser.formatAmount(total, "NGN"),
                            CdcParser.formatAmount(BigDecimal.valueOf(alertThreshold), "NGN")))
                        .eventTime(Instant.ofEpochMilli(wk.window().start()))
                        .meta("date",          date)
                        .meta("totalDebit",    total.doubleValue())
                        .meta("thresholdNgn",  alertThreshold)
                        .build().toJson();
            }, Named.as("daily-build-notification"))
            .to(ABBankStreamsConfig.TOPIC_DAILY_SPEND);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String strField(final GenericRecord record, final String field) {
        final Object v = record.get(field);
        return v == null ? "" : v.toString();
    }

    private static Long parseLong(final String s) {
        if (s == null) return null;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
    }
}
