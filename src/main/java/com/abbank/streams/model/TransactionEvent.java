package com.abbank.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Typed view of a BANKDB.TRANSACTIONS row extracted from an Avro CDC envelope.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionEvent.class);
    private static final long MICROS_TO_MILLIS = 1_000L;

    private long        transactionId;
    private long        accountId;
    private String      transactionRef;
    private String      transactionType;
    private BigDecimal  amount;
    private String      currency;
    private BigDecimal  balanceBefore;
    private BigDecimal  balanceAfter;
    private String      description;
    private String      counterpartyName;
    private String      counterpartyAcct;
    private String      channel;
    private String      transactionStatus;
    private Long        initiatedAtMs;
    private Long        completedAtMs;

    /** Public default constructor for Jackson deserialisation. */
    public TransactionEvent() { }

    public static TransactionEvent fromAvro(final GenericRecord valueRecord) {
        if (valueRecord == null) return null;

        final TransactionEvent e = new TransactionEvent();
        try {
            e.transactionId    = doubleToLong(valueRecord.get("TRANSACTION_ID"));
            e.accountId        = doubleToLong(valueRecord.get("ACCOUNT_ID"));
            e.transactionRef   = str(valueRecord.get("TRANSACTION_REF"));
            e.transactionType  = str(valueRecord.get("TRANSACTION_TYPE"));
            e.amount           = doubleField(valueRecord.get("AMOUNT"));
            e.currency         = str(valueRecord.get("CURRENCY"));
            e.balanceBefore    = doubleField(valueRecord.get("BALANCE_BEFORE"));
            e.balanceAfter     = doubleField(valueRecord.get("BALANCE_AFTER"));
            e.description      = str(valueRecord.get("DESCRIPTION"));
            e.counterpartyName = str(valueRecord.get("COUNTERPARTY_NAME"));
            e.counterpartyAcct = str(valueRecord.get("COUNTERPARTY_ACCT"));
            e.channel          = str(valueRecord.get("CHANNEL"));
            e.transactionStatus = str(valueRecord.get("TRANSACTION_STATUS"));
            e.initiatedAtMs    = microTimestampToMillis(valueRecord.get("INITIATED_AT"));
            e.completedAtMs    = microTimestampToMillis(valueRecord.get("COMPLETED_AT"));
        } catch (Exception ex) {
            LOG.warn("Error mapping TransactionEvent from Avro record: {}", ex.getMessage());
            return null;
        }
        return e;
    }

    public boolean isDebit() {
        return "DEBIT".equals(transactionType)
            || "TRANSFER_OUT".equals(transactionType)
            || "FEE".equals(transactionType)
            || "LOAN_REPAYMENT".equals(transactionType);
    }

    public boolean isCredit() {
        return "CREDIT".equals(transactionType)
            || "TRANSFER_IN".equals(transactionType)
            || "INTEREST".equals(transactionType);
    }

    public boolean isCompleted() {
        return "COMPLETED".equals(transactionStatus);
    }

    public Instant initiatedAt() {
        return initiatedAtMs != null ? Instant.ofEpochMilli(initiatedAtMs) : Instant.now();
    }

    private static long doubleToLong(final Object v) {
        if (v == null) return 0L;
        return (long) ((Number) v).doubleValue();
    }

    private static BigDecimal doubleField(final Object v) {
        if (v == null) return BigDecimal.ZERO;
        return BigDecimal.valueOf(((Number) v).doubleValue()).setScale(4, java.math.RoundingMode.HALF_UP);
    }

    private static Long microTimestampToMillis(final Object v) {
        if (v == null) return null;
        return ((Number) v).longValue() / MICROS_TO_MILLIS;
    }

    private static String str(final Object v) {
        return v == null ? null : v.toString();
    }

    // ── Getters & Setters ──────────────────────────────────────────────────────

    public long getTransactionId() { return transactionId; }
    public void setTransactionId(long transactionId) { this.transactionId = transactionId; }

    public long getAccountId() { return accountId; }
    public void setAccountId(long accountId) { this.accountId = accountId; }

    public String getTransactionRef() { return transactionRef; }
    public void setTransactionRef(String transactionRef) { this.transactionRef = transactionRef; }

    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getCurrency() { return currency != null ? currency : "NGN"; }
    public void setCurrency(String currency) { this.currency = currency; }

    public BigDecimal getBalanceBefore() { return balanceBefore; }
    public void setBalanceBefore(BigDecimal balanceBefore) { this.balanceBefore = balanceBefore; }

    public BigDecimal getBalanceAfter() { return balanceAfter; }
    public void setBalanceAfter(BigDecimal balanceAfter) { this.balanceAfter = balanceAfter; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getCounterpartyName() { return counterpartyName; }
    public void setCounterpartyName(String counterpartyName) { this.counterpartyName = counterpartyName; }

    public String getCounterpartyAcct() { return counterpartyAcct; }
    public void setCounterpartyAcct(String counterpartyAcct) { this.counterpartyAcct = counterpartyAcct; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getTransactionStatus() { return transactionStatus; }
    public void setTransactionStatus(String transactionStatus) { this.transactionStatus = transactionStatus; }

    public Long getInitiatedAtMs() { return initiatedAtMs; }
    public void setInitiatedAtMs(Long initiatedAtMs) { this.initiatedAtMs = initiatedAtMs; }

    public Long getCompletedAtMs() { return completedAtMs; }
    public void setCompletedAtMs(Long completedAtMs) { this.completedAtMs = completedAtMs; }

    @Override
    public String toString() {
        return String.format("TransactionEvent{id=%d, accountId=%d, type=%s, amount=%s, status=%s}",
                transactionId, accountId, transactionType, amount, transactionStatus);
    }
}
