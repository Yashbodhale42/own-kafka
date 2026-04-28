package com.ownkafka.storage;

/**
 * ============================================================================
 * LogRecord — One record in the commit log (in-memory representation).
 * ============================================================================
 *
 * WHAT: A LogRecord represents ONE message stored in the log. It carries:
 *       - offset      → its position in the log (sequential, never changes)
 *       - timestampMs → when it was written (millis since epoch)
 *       - value       → the actual message bytes
 *
 * WHY SEPARATE FROM THE WIRE FORMAT?
 *       The "shape" of a record on disk is bytes — see RecordCodec.
 *       The "shape" of a record in our Java code is this nice immutable record.
 *       Keeping them separate means we can change either side without touching
 *       the other.
 *
 * HOW REAL KAFKA DOES IT:
 *       Real Kafka's record has more fields:
 *       - key bytes (for partitioning by key) — added in Phase 3
 *       - headers (key-value metadata) — added in Phase 5
 *       - CRC checksum (corruption detection)
 *       - producer epoch + sequence (for idempotence) — added in Phase 8
 *       We start minimal and grow.
 *
 * INTERVIEW TIP: "What's stored in a Kafka message?"
 *       → Real Kafka stores: offset, timestamp, key bytes, value bytes,
 *         headers (k/v map), and CRC. The timestamp can be either CreateTime
 *         (when the producer made it) or LogAppendTime (when the broker stored
 *         it) — configurable per topic.
 * ============================================================================
 */
public record LogRecord(long offset, long timestampMs, byte[] value) {

    /**
     * The size this record will take on disk after encoding.
     * Format on disk: [offset:8][timestampMs:8][messageLength:4][value:N]
     * = 20 bytes header + N bytes value.
     *
     * Used by Segment to know "if I add this record, will the segment overflow?"
     */
    public int wireSize() {
        return 8 + 8 + 4 + value.length;
    }
}
