package com.abbank.streams.topology;

import com.abbank.streams.config.ABBankStreamsConfig;
import com.abbank.streams.model.NotificationEvent;
import com.abbank.streams.model.TransactionEvent;
import com.abbank.streams.util.CdcParser;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Stateful balance reconciliation processor — Kafka Streams 3.x edition.
 *
 * <p>Implements {@link FixedKeyProcessor}, the replacement for the deprecated
 * {@code ValueTransformerWithKey} API removed in Kafka Streams 3.x.
 * A {@code FixedKeyProcessor} cannot change the record key (matching the old
 * transformer semantics), and outputs via {@code context.forward()} instead of
 * returning a value.
 *
 * <h2>Schema binding</h2>
 * <ul>
 *   <li>{@code BALANCE_AFTER} is Avro {@code ["null", double]} — already
 *       converted to {@code BigDecimal(4dp)} in {@link TransactionEvent#fromAvro}.</li>
 *   <li>{@code BALANCE_BEFORE} same treatment.</li>
 * </ul>
 *
 * <h2>Logic</h2>
 * <ol>
 *   <li>Read last-known balance from persistent RocksDB KV store (key = account_id string)</li>
 *   <li>Compare against BALANCE_BEFORE from the incoming CDC event</li>
 *   <li>Flag discrepancies exceeding ₦0.01 tolerance as HIGH severity</li>
 *   <li>Update store with new BALANCE_AFTER</li>
 *   <li>Forward a JSON notification string downstream</li>
 * </ol>
 */
public class BalanceReconciliationProcessor
        implements FixedKeyProcessor<String, TransactionEvent, String> {

    private static final Logger LOG = LoggerFactory.getLogger(BalanceReconciliationProcessor.class);

    private static final BigDecimal TOLERANCE = new BigDecimal("0.01");

    private FixedKeyProcessorContext<String, String> context;
    private KeyValueStore<String, String> balanceStore;

    @Override
    public void init(final FixedKeyProcessorContext<String, String> context) {
        this.context = context;
        this.balanceStore = context.getStateStore(ABBankStreamsConfig.STORE_ACCOUNT_BALANCE);
    }

    @Override
    public void process(final FixedKeyRecord<String, TransactionEvent> record) {
        final String accountId = record.key();
        final TransactionEvent txn = record.value();

        if (accountId == null || txn == null) return;

        final BigDecimal reportedBalanceAfter = txn.getBalanceAfter();
        if (reportedBalanceAfter == null || reportedBalanceAfter.compareTo(BigDecimal.ZERO) < 0) {
            return; // can't reconcile without a reported balance
        }

        // ── Read previous stored balance ──────────────────────────────────
        final String storedRaw = balanceStore.get(accountId);
        final BigDecimal storedBalance = storedRaw != null
                ? new BigDecimal(storedRaw)
                : txn.getBalanceBefore(); // bootstrap: use BALANCE_BEFORE on first event

        // ── Detect discrepancy ────────────────────────────────────────────
        boolean hasDiscrepancy = false;
        BigDecimal discrepancy = BigDecimal.ZERO;
        if (storedRaw != null && txn.getBalanceBefore() != null) {
            discrepancy = txn.getBalanceBefore().subtract(storedBalance).abs();
            hasDiscrepancy = discrepancy.compareTo(TOLERANCE) > 0;
            if (hasDiscrepancy) {
                LOG.warn("BALANCE DISCREPANCY: account={} stored={} reportedBefore={} diff={}",
                        accountId, storedBalance, txn.getBalanceBefore(), discrepancy);
            }
        }

        // ── Update store with BALANCE_AFTER ───────────────────────────────
        balanceStore.put(accountId, reportedBalanceAfter.toPlainString());

        // ── Build notification ────────────────────────────────────────────
        final NotificationEvent.Severity severity;
        final String subject;
        final String body;

        if (hasDiscrepancy) {
            severity = NotificationEvent.Severity.HIGH;
            subject  = "⚠️ Balance Discrepancy Detected on Your Account";
            body = String.format(
                "AB Bank detected a balance discrepancy on account %s.\n"
              + "Our records show: %s\n"
              + "Your reported balance before this transaction: %s\n"
              + "Difference: %s\n"
              + "Transaction ref: %s\n"
              + "Our team will investigate. Contact us if you have concerns.",
                accountId,
                CdcParser.formatAmount(storedBalance, txn.getCurrency()),
                CdcParser.formatAmount(txn.getBalanceBefore(), txn.getCurrency()),
                CdcParser.formatAmount(discrepancy, txn.getCurrency()),
                txn.getTransactionRef());
        } else {
            severity = NotificationEvent.Severity.LOW;
            subject  = String.format("✅ Transaction Complete — Balance: %s",
                    CdcParser.formatAmount(reportedBalanceAfter, txn.getCurrency()));
            body = String.format(
                "Your %s of %s has been processed.\n"
              + "Available balance: %s\n"
              + "Reference: %s | Channel: %s | Time: %s",
                txn.getTransactionType(),
                CdcParser.formatAmount(txn.getAmount(), txn.getCurrency()),
                CdcParser.formatAmount(reportedBalanceAfter, txn.getCurrency()),
                txn.getTransactionRef(),
                txn.getChannel(),
                CdcParser.formatTimestampMs(txn.getInitiatedAtMs()));
        }

        final String notification = NotificationEvent.builder()
                .notificationType(NotificationEvent.NotificationType.BALANCE_UPDATE)
                .severity(severity)
                .channel(hasDiscrepancy ? NotificationEvent.Channel.BOTH : NotificationEvent.Channel.SMS)
                .accountId(txn.getAccountId())
                .subject(subject)
                .body(body)
                .eventTime(txn.getInitiatedAtMs() != null
                        ? Instant.ofEpochMilli(txn.getInitiatedAtMs()) : Instant.now())
                .meta("transactionRef",    txn.getTransactionRef())
                .meta("transactionType",   txn.getTransactionType())
                .meta("amountNgn",         txn.getAmount().doubleValue())
                .meta("balanceBefore",     txn.getBalanceBefore().doubleValue())
                .meta("balanceAfter",      reportedBalanceAfter.doubleValue())
                .meta("hasDiscrepancy",    hasDiscrepancy)
                .meta("discrepancyAmount", hasDiscrepancy ? discrepancy.doubleValue() : null)
                .build()
                .toJson();

        // forward() preserves the key — equivalent to the old transformer returning a value
        context.forward(record.withValue(notification));
    }

    @Override
    public void close() { /* State store lifecycle managed by the Streams runtime */ }
}
