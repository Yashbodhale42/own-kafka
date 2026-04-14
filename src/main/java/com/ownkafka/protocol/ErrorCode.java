package com.ownkafka.protocol;

/**
 * ============================================================================
 * ErrorCode — Standard error codes returned by the broker in responses.
 * ============================================================================
 *
 * WHAT: When the broker processes a request, it might succeed or fail.
 *       Instead of sending error messages as strings, Kafka uses numeric
 *       error codes (2 bytes). The client maps these codes to error messages.
 *
 * WHY NUMERIC CODES?
 *       1. Compact: 2 bytes vs potentially long error strings
 *       2. Language-independent: Any client library can map the codes
 *       3. Machine-parseable: Easy to write retry logic based on error codes
 *          (e.g., "if error == LEADER_NOT_AVAILABLE, retry after backoff")
 *
 * REAL KAFKA: Has 90+ error codes! Including OFFSET_OUT_OF_RANGE(1),
 *       CORRUPT_MESSAGE(2), UNKNOWN_TOPIC_OR_PARTITION(3),
 *       LEADER_NOT_AVAILABLE(6), NOT_LEADER_OR_FOLLOWER(6),
 *       REQUEST_TIMED_OUT(7), REPLICA_NOT_AVAILABLE(9), etc.
 *
 * INTERVIEW TIP: "How does Kafka handle errors?"
 *       → Kafka returns numeric error codes per-partition in responses.
 *         This allows a single Produce request to partially succeed
 *         (some partitions OK, some errored). Clients use error codes
 *         to decide: retry? fail? redirect to a different broker?
 * ============================================================================
 */
public enum ErrorCode {

    /** Success — the request was processed without errors */
    NONE(0),

    /**
     * The topic doesn't exist on this broker.
     * Real Kafka code: UNKNOWN_TOPIC_OR_PARTITION(3)
     *
     * In production, this happens when:
     * - A producer sends to a topic that hasn't been created yet
     * - Auto-topic-creation is disabled (recommended for production)
     * - The topic was deleted but the client cache is stale
     */
    UNKNOWN_TOPIC(3),

    /**
     * Catch-all error for unexpected failures.
     * Real Kafka code: UNKNOWN_SERVER_ERROR(-1)
     *
     * In production, this usually means a bug in the broker.
     * Operators watch for this in monitoring dashboards.
     */
    UNKNOWN_ERROR(-1);

    private final short code;

    ErrorCode(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Converts a numeric code back to an ErrorCode enum.
     * Used by the client to interpret the broker's response.
     */
    public static ErrorCode fromCode(short code) {
        for (ErrorCode error : values()) {
            if (error.code == code) {
                return error;
            }
        }
        return UNKNOWN_ERROR;
    }
}
