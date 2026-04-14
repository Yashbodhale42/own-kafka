package com.ownkafka.protocol;

import java.nio.ByteBuffer;

/**
 * ============================================================================
 * Request — A complete request from a client to the broker.
 * ============================================================================
 *
 * WHAT: Combines the request header (what operation + correlation ID)
 *       with the payload (the actual data for that operation).
 *
 * WHY SEPARATE HEADER AND PAYLOAD?
 *       The broker needs to read the header FIRST to know what kind of
 *       request this is. Only then can it parse the payload correctly
 *       (because Produce and Fetch payloads have different formats).
 *
 *       This is a common pattern in network protocols:
 *       - HTTP: headers first, then body
 *       - Kafka: request header, then request body
 *       - TCP itself: TCP header, then data
 *
 * REAL KAFKA: The real Kafka request also includes a "tagged fields"
 *       section for extensibility (added in KIP-482). We skip that for now.
 *
 * The ByteBuffer payload contains the raw bytes specific to each API.
 * For PRODUCE: topic name + message bytes
 * For FETCH: topic name + offset
 * ============================================================================
 */
public record Request(
        /** The parsed header (apiKey, version, correlationId) */
        RequestHeader header,

        /**
         * The raw payload bytes — interpretation depends on apiKey.
         * Using ByteBuffer because it provides convenient methods for
         * reading primitives (getShort, getInt, getLong) from raw bytes.
         *
         * ByteBuffer is used heavily in Kafka's real codebase for the
         * same reason — it's the standard Java way to work with binary data.
         */
        ByteBuffer payload
) {
}
