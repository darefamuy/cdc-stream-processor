package com.abbank.streams.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Formatting utilities for CDC-derived values.
 *
 * <p>Schema-specific conversion rules (derived from abbank-schema.json):
 * <ul>
 *   <li>IDs ({@code TRANSACTION_ID}, {@code ACCOUNT_ID}, {@code CUSTOMER_ID}) are
 *       Avro {@code double} — convert via {@code (long) doubleValue()}</li>
 *   <li>Amounts ({@code AMOUNT}, {@code BALANCE_BEFORE}, {@code BALANCE_AFTER}) are
 *       Avro {@code double} — wrap in {@code BigDecimal.valueOf(double)}</li>
 *   <li>Timestamps ({@code INITIATED_AT}, {@code COMPLETED_AT}, {@code CREATED_AT},
 *       {@code UPDATED_AT}) use {@code io.debezium.time.MicroTimestamp} — stored as
 *       epoch <em>microseconds</em> (long). Divide by 1000 for epoch milliseconds.</li>
 *   <li>{@code OPENED_DATE}, {@code CLOSED_DATE}, {@code DATE_OF_BIRTH} use
 *       {@code io.debezium.time.Timestamp} — stored as epoch <em>milliseconds</em>.</li>
 * </ul>
 */
public final class CdcParser {

    private static final Logger LOG = LoggerFactory.getLogger(CdcParser.class);
    private static final ZoneId LAGOS = ZoneId.of("Africa/Lagos");
    private static final DateTimeFormatter DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(LAGOS);

    private CdcParser() { }

    /**
     * Format a BigDecimal amount with the appropriate currency symbol.
     * NGN amounts are prefixed with ₦; other currencies use the ISO code.
     */
    public static String formatAmount(final BigDecimal amount, final String currency) {
        if (amount == null) return "₦0.00";
        final String symbol = "NGN".equalsIgnoreCase(currency) ? "₦" : (currency + " ");
        return symbol + String.format("%,.2f", amount);
    }

    /**
     * Convert epoch milliseconds to a human-readable timestamp in Lagos time.
     */
    public static String formatTimestampMs(final Long epochMs) {
        if (epochMs == null) return "N/A";
        return DT_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    /**
     * Convert a MicroTimestamp value (epoch microseconds as long) to epoch milliseconds.
     * Returns null if the input is null.
     */
    public static Long microTimestampToMillis(final Object v) {
        if (v == null) return null;
        return ((Number) v).longValue() / 1_000L;
    }

    /**
     * Safe extraction of a double-typed Avro field as {@code long}.
     * Oracle NUMBER primary keys are mapped to Avro double by the XStream connector.
     */
    public static long doubleToLong(final Object v) {
        if (v == null) return 0L;
        return (long) ((Number) v).doubleValue();
    }

    /**
     * Safe extraction of a double-typed Avro field as {@code BigDecimal}.
     * Oracle NUMBER(18,4) amounts are mapped to Avro double.
     */
    public static BigDecimal doubleToBigDecimal(final Object v) {
        if (v == null) return BigDecimal.ZERO;
        return BigDecimal.valueOf(((Number) v).doubleValue())
                .setScale(4, java.math.RoundingMode.HALF_UP);
    }
}
