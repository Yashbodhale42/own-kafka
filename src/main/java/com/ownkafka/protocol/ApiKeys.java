package com.ownkafka.protocol;

/**
 * ============================================================================
 * ApiKeys — Identifies the type of request being sent to the broker.
 * ============================================================================
 *
 * WHAT: Every request to a Kafka broker starts with an "API Key" — a number
 *       that tells the broker what operation the client wants to perform.
 *       Think of it like HTTP methods (GET, POST) but as numbers.
 *
 * WHY NUMBERS, NOT STRINGS?
 *       Binary protocols use numbers instead of strings because:
 *       1. A number (2 bytes) is WAY smaller than a string like "PRODUCE" (7 bytes)
 *       2. Comparing numbers is faster than comparing strings
 *       3. No encoding issues (UTF-8, ASCII, etc.)
 *       At scale (millions of requests/sec), these savings add up massively.
 *
 * REAL KAFKA: Has 60+ API keys! Including Produce(0), Fetch(1), ListOffsets(2),
 *       Metadata(3), OffsetCommit(8), OffsetFetch(9), FindCoordinator(10),
 *       JoinGroup(11), Heartbeat(12), and many more. We start with just 2.
 *
 * INTERVIEW TIP: "What happens when a producer sends a message to Kafka?"
 *       → The producer serializes the message, wraps it in a Produce request
 *         (API key 0), sends it over TCP to the broker. The broker parses the
 *         API key, routes to the produce handler, appends to the commit log,
 *         and sends back an acknowledgment with the offset.
 * ============================================================================
 */
public enum ApiKeys {

    /**
     * PRODUCE (API Key 0): Client wants to write/publish messages to a topic.
     *
     * In real Kafka, this is the most critical operation. Producers use this
     * to send messages. The broker appends them to the commit log and returns
     * the offset (position) where the message was stored.
     *
     * Real-world example: At Uber, every ride request triggers a PRODUCE
     * to a "ride-requests" topic. Millions per day.
     */
    PRODUCE(0),

    /**
     * FETCH (API Key 1): Client wants to read/consume messages from a topic.
     *
     * In real Kafka, consumers continuously send FETCH requests to pull
     * new messages. They specify "give me messages starting from offset X".
     *
     * Real-world example: A fraud detection service FETCH-es from the
     * "transactions" topic to analyze each payment in real-time.
     */
    FETCH(1);

    // The numeric code sent over the wire (2 bytes in our protocol)
    private final short code;

    ApiKeys(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Converts a numeric code back to an ApiKeys enum.
     * This is called when the server receives a request and needs to figure
     * out what type of request it is.
     *
     * @throws IllegalArgumentException if the code doesn't match any known API key
     *         (In real Kafka, this would return an UNSUPPORTED_VERSION error)
     */
    public static ApiKeys fromCode(short code) {
        for (ApiKeys key : values()) {
            if (key.code == code) {
                return key;
            }
        }
        throw new IllegalArgumentException("Unknown API key: " + code);
    }
}
