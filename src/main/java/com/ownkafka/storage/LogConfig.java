package com.ownkafka.storage;

import java.nio.file.Path;

/**
 * ============================================================================
 * LogConfig — Configuration for the disk-based commit log.
 * ============================================================================
 *
 * WHAT: This is a tiny "settings bag" — all the knobs the storage layer
 *       cares about, bundled into one immutable record.
 *
 *       - dataDir            → where on disk to put the segment files
 *       - segmentSizeBytes   → roll a new segment when active one exceeds this
 *       - retentionBytes     → max total size of a topic's data on disk
 *       - retentionMillis    → max age of a segment (older ones get deleted)
 *       - indexIntervalBytes → write an index entry every N bytes appended
 *
 * WHY A RECORD? Java records are perfect for configuration — immutable,
 *       no boilerplate, instantly comparable with equals(). One config
 *       object passes through every layer.
 *
 * HOW REAL KAFKA DOES IT:
 *       Real Kafka has ~200 config keys (server.properties), parsed into
 *       LogConfig and KafkaConfig classes. We start with just 5 — add more
 *       as later phases need them.
 *
 *       Real Kafka defaults (for context):
 *       - log.segment.bytes = 1 GB
 *       - log.retention.bytes = -1 (disabled by default; size-based)
 *       - log.retention.hours = 168 (7 days)
 *       - log.index.interval.bytes = 4096 (4 KB)
 *
 * INTERVIEW TIP: "How do you tune a Kafka cluster?"
 *       → Two of the most important knobs are segment size and retention:
 *         smaller segments = faster recovery, more frequent rolls;
 *         larger segments = better throughput, slower deletion.
 *         Retention controls how much disk Kafka uses per topic.
 * ============================================================================
 */
public record LogConfig(
        /** Root data directory. Each topic gets its own subfolder here. */
        Path dataDir,

        /** Active segment is rolled (closed, new one created) when its size exceeds this. */
        long segmentSizeBytes,

        /** Maximum total bytes per topic on disk. Older segments deleted to enforce. */
        long retentionBytes,

        /** Maximum age of a segment in milliseconds. Older segments deleted. */
        long retentionMillis,

        /** Write an OffsetIndex entry every N bytes of log written (sparse index). */
        int indexIntervalBytes
) {

    /**
     * Compact constructor — validates inputs.
     * Compact constructors run BEFORE the regular constructor and let you
     * validate or normalize parameters cleanly.
     */
    public LogConfig {
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir cannot be null");
        }
        if (segmentSizeBytes <= 0) {
            throw new IllegalArgumentException("segmentSizeBytes must be positive: " + segmentSizeBytes);
        }
        // Important: relativeOffset and filePosition in OffsetIndex are 4-byte ints.
        // If a segment grows past 2 GB, the int overflows. So cap segments at 2 GB.
        if (segmentSizeBytes >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("segmentSizeBytes must be < 2 GB (int overflow): " + segmentSizeBytes);
        }
        if (retentionBytes <= 0) {
            throw new IllegalArgumentException("retentionBytes must be positive: " + retentionBytes);
        }
        if (retentionMillis <= 0) {
            throw new IllegalArgumentException("retentionMillis must be positive: " + retentionMillis);
        }
        if (indexIntervalBytes <= 0) {
            throw new IllegalArgumentException("indexIntervalBytes must be positive: " + indexIntervalBytes);
        }
    }

    /**
     * Production-style defaults.
     * - 1 MB segments (real Kafka uses 1 GB; we use smaller for visibility during learning)
     * - 100 MB total retention per topic
     * - 7 days retention age
     * - 4 KB index interval (matches real Kafka)
     */
    public static LogConfig defaults() {
        return new LogConfig(
                Path.of("data"),
                1024L * 1024L,                       // 1 MB
                100L * 1024L * 1024L,                // 100 MB
                7L * 24L * 60L * 60L * 1000L,        // 7 days
                4 * 1024                             // 4 KB
        );
    }

    /**
     * Tiny config for tests — forces frequent segment rolls and
     * frequent index entries so we exercise those code paths.
     */
    public static LogConfig forTesting(Path tmpDir) {
        return new LogConfig(
                tmpDir,
                1024L,                               // 1 KB segments — rolls quickly
                10L * 1024L,                         // 10 KB retention
                Long.MAX_VALUE,                      // disable age-based retention by default
                64                                   // 64 B index interval — many index entries
        );
    }
}
