package com.abbank.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Map;

/**
 * Factory for all Avro Serdes used in the AB Bank topology.
 */
public final class AvroSerdes {

    private static SchemaRegistryClient mockClient;

    private AvroSerdes() { }

    /**
     * Set a mock SchemaRegistryClient to be used by all serdes created by this factory.
     * Used exclusively for unit testing with TopologyTestDriver.
     */
    public static void setMockClient(final SchemaRegistryClient client) {
        mockClient = client;
    }

    /**
     * Create a configured GenericAvroSerde (key or value).
     *
     * @param schemaRegistryUrl  e.g. {@code http://schema-registry:8081}
     * @param isKey              {@code true} for key serde, {@code false} for value serde
     */
    public static GenericAvroSerde genericAvro(final String schemaRegistryUrl, final boolean isKey) {
        final GenericAvroSerde serde = mockClient != null
                ? new GenericAvroSerde(mockClient)
                : new GenericAvroSerde();

        final Map<String, String> config = Collections.singletonMap(
                "schema.registry.url", schemaRegistryUrl);
        serde.configure(config, isKey);
        return serde;
    }

    /**
     * Value serde for all CDC Envelope records
     * (TRANSACTIONS, ACCOUNTS, CUSTOMERS, TRANSACTION_AUDIT).
     */
    public static Serde<GenericRecord> envelopeSerde(final String schemaRegistryUrl) {
        return genericAvro(schemaRegistryUrl, false);
    }

    /**
     * Key serde for all CDC key records (single {@code double} field per table).
     */
    public static Serde<GenericRecord> keySerde(final String schemaRegistryUrl) {
        return genericAvro(schemaRegistryUrl, true);
    }

    /**
     * String serde — used for re-keyed streams (account_id as String) and
     * all output notification topics.
     */
    public static Serde<String> stringSerde() {
        return Serdes.String();
    }
}
