package com.abbank.streams.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Stateful window for velocity fraud detection.
 *
 * <p>Tracks a sliding window of recent transaction timestamps and cumulative
 * amount per account within the configured window. Serialised into the
 * Kafka Streams state store as JSON bytes.
 */
public class VelocityState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Timestamps of recent transactions within the window. */
    private final Deque<Long> transactionTimestamps;

    /** Cumulative amount of transactions within the window. */
    private BigDecimal cumulativeAmount;

    /** Timestamp of the most recently seen transaction. */
    private long lastTransactionMs;

    public VelocityState() {
        this.transactionTimestamps = new ArrayDeque<>();
        this.cumulativeAmount      = BigDecimal.ZERO;
        this.lastTransactionMs     = 0L;
    }

    /**
     * Record a new transaction, evicting entries that fall outside the window.
     *
     * @param timestampMs     epoch-millisecond timestamp of the transaction
     * @param amount          transaction amount
     * @param windowSizeMs    sliding window size in milliseconds
     */
    public void record(final long timestampMs, final BigDecimal amount, final long windowSizeMs) {
        final long windowStart = timestampMs - windowSizeMs;

        // Evict expired entries from the front of the deque
        while (!transactionTimestamps.isEmpty() && transactionTimestamps.peekFirst() < windowStart) {
            transactionTimestamps.pollFirst();
        }

        transactionTimestamps.addLast(timestampMs);
        cumulativeAmount = cumulativeAmount.add(amount);
        lastTransactionMs = timestampMs;
    }

    public int getTransactionCount()       { return transactionTimestamps.size(); }
    public BigDecimal getCumulativeAmount() { return cumulativeAmount; }
    public long getLastTransactionMs()     { return lastTransactionMs; }

    @Override
    public String toString() {
        return "VelocityState{count=" + transactionTimestamps.size()
             + ", cumulative=" + cumulativeAmount + "}";
    }
}
