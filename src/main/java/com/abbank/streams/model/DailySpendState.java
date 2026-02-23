package com.abbank.streams.model;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Accumulates debit totals for a customer on a given calendar day.
 * Key in the state store: {@code customerId:YYYY-MM-DD}.
 */
public class DailySpendState {

    private BigDecimal totalDebit;
    private int        transactionCount;
    private LocalDate  date;
    private long       lastUpdatedMs;

    public DailySpendState() {
        this.totalDebit       = BigDecimal.ZERO;
        this.transactionCount = 0;
        this.date             = LocalDate.now();
        this.lastUpdatedMs    = System.currentTimeMillis();
    }

    public void addDebit(final BigDecimal amount) {
        totalDebit        = totalDebit.add(amount);
        transactionCount++;
        lastUpdatedMs     = System.currentTimeMillis();
    }

    public BigDecimal getTotalDebit()      { return totalDebit; }
    public int        getTransactionCount() { return transactionCount; }
    public LocalDate  getDate()            { return date; }
    public long       getLastUpdatedMs()   { return lastUpdatedMs; }
    public void       setDate(final LocalDate d) { this.date = d; }
}
