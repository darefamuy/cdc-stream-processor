package com.abbank.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Typed view of a BANKDB.ACCOUNTS row from the Avro CDC envelope.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountEvent {

    private static final Logger LOG = LoggerFactory.getLogger(AccountEvent.class);

    private long   accountId;
    private long   customerId;
    private String accountNumber;
    private String accountType;
    private String currency;
    private double balance;
    private double availableBalance;
    private String accountStatus;

    public AccountEvent() { }

    public static AccountEvent fromAvro(final GenericRecord valueRecord) {
        if (valueRecord == null) return null;
        final AccountEvent e = new AccountEvent();
        try {
            e.accountId        = doubleToLong(valueRecord.get("ACCOUNT_ID"));
            e.customerId       = doubleToLong(valueRecord.get("CUSTOMER_ID"));
            e.accountNumber    = str(valueRecord.get("ACCOUNT_NUMBER"));
            e.accountType      = str(valueRecord.get("ACCOUNT_TYPE"));
            e.currency         = str(valueRecord.get("CURRENCY"));
            e.balance          = valueRecord.get("BALANCE") != null
                                   ? ((Number) valueRecord.get("BALANCE")).doubleValue() : 0.0;
            e.availableBalance = valueRecord.get("AVAILABLE_BALANCE") != null
                                   ? ((Number) valueRecord.get("AVAILABLE_BALANCE")).doubleValue() : 0.0;
            e.accountStatus    = str(valueRecord.get("ACCOUNT_STATUS"));
        } catch (Exception ex) {
            LOG.warn("Error mapping AccountEvent from Avro: {}", ex.getMessage());
            return null;
        }
        return e;
    }

    private static long doubleToLong(final Object v) {
        if (v == null) return 0L;
        return (long) ((Number) v).doubleValue();
    }

    private static String str(final Object v) {
        return v == null ? null : v.toString();
    }

    // ── Getters & Setters ──────────────────────────────────────────────────────

    public long getAccountId() { return accountId; }
    public void setAccountId(long accountId) { this.accountId = accountId; }

    public long getCustomerId() { return customerId; }
    public void setCustomerId(long customerId) { this.customerId = customerId; }

    public String getAccountNumber() { return accountNumber; }
    public void setAccountNumber(String accountNumber) { this.accountNumber = accountNumber; }

    public String getAccountType() { return accountType; }
    public void setAccountType(String accountType) { this.accountType = accountType; }

    public String getCurrency() { return currency != null ? currency : "NGN"; }
    public void setCurrency(String currency) { this.currency = currency; }

    public double getBalance() { return balance; }
    public void setBalance(double balance) { this.balance = balance; }

    public double getAvailableBalance() { return availableBalance; }
    public void setAvailableBalance(double availableBalance) { this.availableBalance = availableBalance; }

    public String getAccountStatus() { return accountStatus; }
    public void setAccountStatus(String accountStatus) { this.accountStatus = accountStatus; }

    @Override
    public String toString() {
        return String.format("AccountEvent{id=%d, customerId=%d, number=%s, balance=%.4f}",
                accountId, customerId, accountNumber, balance);
    }
}
