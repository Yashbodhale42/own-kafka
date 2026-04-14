package com.ownkafka.protocol;

/**
 * ============================================================================
 * RequestHeader — The header that starts every request sent to the broker.
 * ============================================================================
 *
 * WHAT: Every Kafka request begins with a header containing:
 *       - apiKey: WHAT operation (Produce? Fetch? Metadata?)
 *       - apiVersion: WHICH VERSION of that operation's format
 *       - correlationId: A unique ID to match this request to its response
 *
 * WHY CORRELATION IDs?
 *       TCP is a stream — bytes flow continuously. When a client sends
 *       multiple requests, responses might come back in any order (pipelining).
 *       The correlationId lets the client match: "This response is for
 *       request #42 that I sent earlier."
 *
 *       Think of it like a restaurant order number. You place order #42,
 *       and when the waiter calls "Order 42!", you know it's yours.
 *
 * WHY API VERSIONING?
 *       Kafka evolves its protocol over time. Produce v0 is different from
 *       Produce v8. Versioning allows old clients to talk to new brokers
 *       and vice versa. This is how Kafka achieves rolling upgrades —
 *       you can upgrade brokers one-by-one without downtime.
 *
 * INTERVIEW TIP: "How does Kafka achieve zero-downtime upgrades?"
 *       → Protocol versioning! Each API has multiple versions. During a
 *         rolling upgrade, new brokers still understand old request formats.
 *         Clients negotiate the version via the ApiVersions request.
 *
 * NOTE: This is a Java "record" (Java 16+). Records are immutable data
 *       carriers — perfect for protocol headers. The compiler auto-generates
 *       constructor, getters, equals(), hashCode(), and toString().
 * ============================================================================
 */
public record RequestHeader(
        /** Which API operation to perform (PRODUCE=0, FETCH=1, etc.) */
        ApiKeys apiKey,

        /** Protocol version for this API (allows backward compatibility) */
        short apiVersion,

        /**
         * Unique ID to correlate this request with its response.
         * The client generates this (usually incrementing counter).
         * The broker echoes it back in the response header.
         */
        int correlationId
) {
    /**
     * Size of the header when serialized to bytes.
     * apiKey(2) + apiVersion(2) + correlationId(4) = 8 bytes
     *
     * In real Kafka, the header also includes a clientId string,
     * which adds variable length. We keep it simple for now.
     */
    public static final int HEADER_SIZE = 2 + 2 + 4; // 8 bytes
}
