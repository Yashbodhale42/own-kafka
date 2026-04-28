package com.ownkafka.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CommitLog — manages all segments for one topic.
 */
class CommitLogTest {

    @TempDir
    Path tempDir;

    private LogConfig config(int segmentSize) {
        return new LogConfig(tempDir, segmentSize, 100L * 1024 * 1024, Long.MAX_VALUE, 32);
    }

    @Test
    @DisplayName("Append assigns sequential offsets")
    void appendsSequential() throws IOException {
        try (CommitLog log = CommitLog.openOrCreate("orders", config(1024))) {
            assertEquals(0L, log.append("a".getBytes()));
            assertEquals(1L, log.append("b".getBytes()));
            assertEquals(2L, log.append("c".getBytes()));
            assertEquals(3L, log.endOffset());
        }
    }

    @Test
    @DisplayName("Active segment rolls when it exceeds segmentSizeBytes")
    void rollsSegmentWhenFull() throws IOException {
        // Tiny segment — each record is 21 bytes (20 header + 1), so ~3 records per segment
        try (CommitLog log = CommitLog.openOrCreate("orders", config(64))) {
            for (int i = 0; i < 10; i++) {
                log.append(new byte[]{(byte) i});
            }
        }

        // Should have rolled at least 3 segments by now
        Path topicDir = tempDir.resolve("orders");
        long segmentCount = Files.list(topicDir)
                .filter(p -> p.toString().endsWith(".log"))
                .count();
        assertTrue(segmentCount >= 3, "Expected at least 3 segments, got " + segmentCount);
    }

    @Test
    @DisplayName("Read spans multiple segments correctly")
    void readSpansSegments() throws IOException {
        try (CommitLog log = CommitLog.openOrCreate("orders", config(64))) {
            for (int i = 0; i < 20; i++) {
                log.append(("msg-" + i).getBytes());
            }

            List<LogRecord> records = log.read(0L, 100, 4096);
            assertEquals(20, records.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(i, records.get(i).offset());
                assertEquals("msg-" + i, new String(records.get(i).value()));
            }
        }
    }

    @Test
    @DisplayName("End offset continues across segment rolls")
    void endOffsetAcrossRolls() throws IOException {
        try (CommitLog log = CommitLog.openOrCreate("orders", config(64))) {
            for (int i = 0; i < 50; i++) {
                log.append(("msg-" + i).getBytes());
            }
            assertEquals(50L, log.endOffset());
        }
    }

    @Test
    @DisplayName("Reopen recovers all segments and correct end offset")
    void reopenRecoversAll() throws IOException {
        try (CommitLog log = CommitLog.openOrCreate("orders", config(64))) {
            for (int i = 0; i < 30; i++) {
                log.append(("msg-" + i).getBytes());
            }
        }

        try (CommitLog reopened = CommitLog.openOrCreate("orders", config(64))) {
            assertEquals(30L, reopened.endOffset());
            List<LogRecord> records = reopened.read(0L, 100, 4096);
            assertEquals(30, records.size());
            for (int i = 0; i < 30; i++) {
                assertEquals("msg-" + i, new String(records.get(i).value()));
            }
        }
    }

    @Test
    @DisplayName("Retention by size deletes oldest segments")
    void retentionDeletesOldSegments() throws IOException {
        // Tiny retention so we trigger deletion
        LogConfig cfg = new LogConfig(tempDir, 64L, 200L, Long.MAX_VALUE, 32);

        try (CommitLog log = CommitLog.openOrCreate("orders", cfg)) {
            // Produce way more than retention allows
            for (int i = 0; i < 100; i++) {
                log.append(("msg-" + i).getBytes());
            }

            int deleted = log.enforceRetention();
            assertTrue(deleted > 0, "Expected some segments to be deleted");
            assertTrue(log.totalSizeBytes() <= cfg.retentionBytes() * 2,
                    "Total size after retention should be reasonable");
        }
    }

    @Test
    @DisplayName("Retention never deletes the active segment")
    void retentionNeverDeletesActive() throws IOException {
        // Retention 1 byte (super aggressive). Active should still survive.
        LogConfig cfg = new LogConfig(tempDir, 1024L, 1L, 1L, 32);

        try (CommitLog log = CommitLog.openOrCreate("orders", cfg)) {
            log.append("hello".getBytes());

            // Sleep a bit to age the segment past retentionMillis
            try { Thread.sleep(10); } catch (InterruptedException ignored) {}

            log.enforceRetention();

            // Even with aggressive retention, the active segment survives
            assertTrue(log.totalSizeBytes() > 0);
            assertEquals(1L, log.endOffset());
        }
    }

    @Test
    @DisplayName("Retention always keeps at least one segment")
    void retentionKeepsAtLeastOne() throws IOException {
        LogConfig cfg = new LogConfig(tempDir, 64L, 1L, 1L, 32);

        try (CommitLog log = CommitLog.openOrCreate("orders", cfg)) {
            for (int i = 0; i < 20; i++) {
                log.append(("msg-" + i).getBytes());
            }
            log.enforceRetention();

            // We should have at least 1 segment file remaining
            long remaining = Files.list(tempDir.resolve("orders"))
                    .filter(p -> p.toString().endsWith(".log"))
                    .count();
            assertTrue(remaining >= 1);
        }
    }

    @Test
    @DisplayName("Read from offset before startOffset returns from startOffset")
    void readBeforeStartOffset() throws IOException {
        try (CommitLog log = CommitLog.openOrCreate("orders", config(1024))) {
            log.append("a".getBytes());
            log.append("b".getBytes());

            // Request from offset -1 (clamped to start)
            List<LogRecord> records = log.read(-100L, 10, 1024);
            assertEquals(2, records.size());
        }
    }
}
