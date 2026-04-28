package com.ownkafka.storage;

import java.io.IOException;

/**
 * ============================================================================
 * CorruptRecordException — Thrown when a record on disk is malformed.
 * ============================================================================
 *
 * WHAT: Custom exception we throw when reading the log encounters bytes that
 *       don't make sense as a record — usually a "torn write".
 *
 * WHAT IS A TORN WRITE?
 *       Imagine the broker is in the middle of writing a record to disk:
 *
 *         [offset:8][timestamp:8][length:4][bytes 0..N]
 *                                                     ↑
 *                                                     KILL -9 happens here!
 *
 *       The OS flushed the first 30 bytes to disk but not the last 5.
 *       When we restart and read this file, we hit EOF mid-record.
 *       That's a torn write. We throw this exception, the recovery code
 *       catches it, and truncates the file to the last clean boundary.
 *
 * HOW REAL KAFKA HANDLES THIS:
 *       Real Kafka uses CRC-32C checksums in every record batch. On read,
 *       it verifies the checksum; mismatch → CorruptRecordException →
 *       truncate the rest of the segment. Our simpler design just uses
 *       the length field (if there aren't enough bytes for the declared
 *       length, the record is torn).
 *
 *       In Phase 5 when we adopt real Kafka's record format, we'll add
 *       proper CRC checking.
 *
 * INTERVIEW TIP: "How does Kafka recover from a broker crash?"
 *       → On startup, Kafka scans each segment file from the beginning,
 *         validating each record's CRC. When it hits a torn record, it
 *         truncates the file to that point and continues serving.
 *         No external coordination needed — recovery is purely local.
 * ============================================================================
 */
public class CorruptRecordException extends IOException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
