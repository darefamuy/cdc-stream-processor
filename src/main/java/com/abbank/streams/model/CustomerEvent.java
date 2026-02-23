package com.abbank.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Typed view of a BANKDB.CUSTOMERS row from the Avro CDC envelope.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerEvent {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerEvent.class);

    private long   customerId;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;
    private String city;
    private String country;
    private String customerStatus;

    public CustomerEvent() { }

    public static CustomerEvent fromAvro(final GenericRecord valueRecord) {
        if (valueRecord == null) return null;
        final CustomerEvent c = new CustomerEvent();
        try {
            c.customerId      = doubleToLong(valueRecord.get("CUSTOMER_ID"));
            c.firstName       = str(valueRecord.get("FIRST_NAME"));
            c.lastName        = str(valueRecord.get("LAST_NAME"));
            c.email           = str(valueRecord.get("EMAIL"));
            c.phone           = str(valueRecord.get("PHONE"));
            c.city            = str(valueRecord.get("CITY"));
            c.country         = str(valueRecord.get("COUNTRY"));
            c.customerStatus  = str(valueRecord.get("CUSTOMER_STATUS"));
        } catch (Exception ex) {
            LOG.warn("Error mapping CustomerEvent from Avro: {}", ex.getMessage());
            return null;
        }
        return c;
    }

    public String getFullName() {
        return (firstName != null ? firstName : "") + " " + (lastName != null ? lastName : "");
    }

    private static long doubleToLong(final Object v) {
        if (v == null) return 0L;
        return (long) ((Number) v).doubleValue();
    }

    private static String str(final Object v) {
        return v == null ? null : v.toString();
    }

    // ── Getters & Setters ──────────────────────────────────────────────────────

    public long getCustomerId() { return customerId; }
    public void setCustomerId(long customerId) { this.customerId = customerId; }

    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getCustomerStatus() { return customerStatus; }
    public void setCustomerStatus(String customerStatus) { this.customerStatus = customerStatus; }

    @Override
    public String toString() {
        return String.format("CustomerEvent{id=%d, name=%s, email=%s, status=%s}",
                customerId, getFullName(), email, customerStatus);
    }
}
