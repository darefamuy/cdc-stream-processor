package com.abbank.streams.model;

import org.apache.avro.generic.GenericRecord;

/**
 * Utility façade over the Avro CDC Envelope GenericRecord.
 *
 * <h2>Avro Envelope structure (all four tables share this layout)</h2>
 * <pre>
 * Envelope {
 *   before:      ["null", Value]       // row state before the change
 *   after:       ["null", Value]       // row state after the change
 *   source:      Source                // connector metadata (table, scn, ts_ms …)
 *   transaction: ["null", event.block] // transaction block metadata
 *   op:          string                // "c"=insert "u"=update "d"=delete "r"=snapshot
 *   ts_ms:       ["null", long]        // wall-clock ms when connector processed event
 *   ts_us:       ["null", long]        // same in microseconds
 *   ts_ns:       ["null", long]        // same in nanoseconds
 * }
 * </pre>
 *
 * <p>This class is a stateless wrapper — it does not copy or cache the record;
 * it delegates directly to the underlying GenericRecord for zero-copy access.
 */
public final class CdcEnvelope {

    private final GenericRecord envelope;

    public CdcEnvelope(final GenericRecord envelope) {
        if (envelope == null) throw new IllegalArgumentException("envelope must not be null");
        this.envelope = envelope;
    }

    // ── Operation type ─────────────────────────────────────────────────────────

    public String getOp() {
        final Object op = envelope.get("op");
        return op == null ? "" : op.toString();
    }

    public boolean isInsert()   { return "c".equals(getOp()); }
    public boolean isUpdate()   { return "u".equals(getOp()); }
    public boolean isDelete()   { return "d".equals(getOp()); }
    public boolean isSnapshot() { return "r".equals(getOp()); }
    public boolean isUpsert()   { return isInsert() || isUpdate() || isSnapshot(); }

    // ── Row accessors ──────────────────────────────────────────────────────────

    /** The Value sub-record representing the row state BEFORE the change, or null. */
    public GenericRecord getBefore() { return (GenericRecord) envelope.get("before"); }

    /** The Value sub-record representing the row state AFTER the change, or null. */
    public GenericRecord getAfter()  { return (GenericRecord) envelope.get("after"); }

    /**
     * Returns the most useful row for this operation:
     * {@code after} for inserts / updates / snapshots,
     * {@code before} for deletes.
     */
    public GenericRecord effectiveRow() {
        return isDelete() ? getBefore() : getAfter();
    }

    // ── Source metadata ────────────────────────────────────────────────────────

    /**
     * Wall-clock milliseconds when the XStream connector emitted this event.
     * Field: {@code ts_ms} — Avro type {@code ["null", long]}.
     */
    public Long getTsMs() {
        final Object v = envelope.get("ts_ms");
        return v == null ? null : ((Number) v).longValue();
    }

    /**
     * Source sub-record fields: {@code table}, {@code schema}, {@code scn},
     * {@code txId}, {@code lcr_position}, {@code user_name}.
     * Namespace: {@code io.confluent.connect.oracle.xstream}.
     */
    public GenericRecord getSource() {
        return (GenericRecord) envelope.get("source");
    }

    /** Convenience: return the Oracle table name from the source sub-record. */
    public String getSourceTable() {
        final GenericRecord src = getSource();
        if (src == null) return null;
        final Object t = src.get("table");
        return t == null ? null : t.toString();
    }

    /** The underlying Avro GenericRecord (for advanced access). */
    public GenericRecord raw() { return envelope; }
}
