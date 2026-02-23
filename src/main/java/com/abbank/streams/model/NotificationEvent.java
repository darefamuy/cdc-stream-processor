package com.abbank.streams.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Canonical notification event written to {@code abbank.notifications.outbox}.
 *
 * <p>The downstream Notification Consumer reads this topic and routes events
 * to the appropriate channel adapter (email / SMS) based on {@code notificationType}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public enum NotificationType {
        FRAUD_ALERT,
        HIGH_VALUE_ALERT,
        BALANCE_UPDATE,
        DORMANCY_ALERT,
        DAILY_SPEND_SUMMARY
    }

    public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }
    public enum Channel  { EMAIL, SMS, BOTH }

    private String            notificationId;
    private NotificationType  notificationType;
    private Severity          severity;
    private Channel           channel;
    private Long              accountId;
    private Long              customerId;   // resolved by joining accounts KTable
    private String            accountNumber;
    private String            subject;
    private String            body;
    private Instant           eventTime;
    private Instant           generatedAt;
    private Map<String, Object> metadata;

    private NotificationEvent() { }

    // ── Builder ───────────────────────────────────────────────────────────────
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private final NotificationEvent event = new NotificationEvent();
        private final Map<String, Object> metadata = new LinkedHashMap<>();

        public Builder notificationType(final NotificationType t) {
            event.notificationType = t; return this;
        }
        public Builder severity(final Severity s) { event.severity = s; return this; }
        public Builder channel(final Channel c)    { event.channel = c; return this; }
        public Builder accountId(final Long id)    { event.accountId = id; return this; }
        public Builder customerId(final Long id)   { event.customerId = id; return this; }
        public Builder accountNumber(final String n) { event.accountNumber = n; return this; }
        public Builder subject(final String s)     { event.subject = s; return this; }
        public Builder body(final String b)        { event.body = b; return this; }
        public Builder eventTime(final Instant t)  { event.eventTime = t; return this; }
        public Builder meta(final String k, final Object v) { metadata.put(k, v); return this; }

        public NotificationEvent build() {
            event.notificationId = java.util.UUID.randomUUID().toString();
            event.generatedAt    = Instant.now();
            event.metadata       = metadata.isEmpty() ? null : metadata;
            if (event.notificationType == null) throw new IllegalStateException("notificationType required");
            if (event.severity == null)         event.severity = Severity.MEDIUM;
            if (event.channel  == null)         event.channel  = Channel.BOTH;
            return event;
        }
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialise NotificationEvent", e);
        }
    }

    public static NotificationEvent fromJson(final String json) {
        try {
            return MAPPER.readValue(json, NotificationEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialise NotificationEvent", e);
        }
    }

    // ── Getters ───────────────────────────────────────────────────────────────
    public String           getNotificationId()   { return notificationId; }
    public NotificationType getNotificationType() { return notificationType; }
    public Severity         getSeverity()         { return severity; }
    public Channel          getChannel()          { return channel; }
    public Long             getAccountId()        { return accountId; }
    public Long             getCustomerId()       { return customerId; }
    public String           getAccountNumber()    { return accountNumber; }
    public String           getSubject()          { return subject; }
    public String           getBody()             { return body; }
    public Instant          getEventTime()        { return eventTime; }
    public Instant          getGeneratedAt()      { return generatedAt; }
    public Map<String, Object> getMetadata()      { return metadata; }

    @Override
    public String toString() {
        return "NotificationEvent{id=" + notificationId
             + ", type=" + notificationType
             + ", severity=" + severity
             + ", accountId=" + accountId + "}";
    }
}
