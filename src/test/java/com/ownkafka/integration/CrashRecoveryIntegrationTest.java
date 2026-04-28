package com.ownkafka.integration;

import com.ownkafka.storage.LogConfig;
import com.ownkafka.storage.LogManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ============================================================================
 * Crash Recovery Integration Tests — the headline tests of Phase 2.
 *
 * These verify the core promise of disk-based storage:
 * "Restart the broker and your data is still there."
 * ============================================================================
 */
class CrashRecoveryIntegrationTest {

    @TempDir
    Path tempDir;

    /** Tiny config that forces lots of segment rolls (tests cross-segment recovery). */
    private LogConfig smallSegmentsConfig() {
        return new LogConfig(tempDir, 4 * 1024L, 100L * 1024 * 1024, Long.MAX_VALUE, 64);
    }

    @Test
    @DisplayName("Write 1000 messages, close cleanly, reopen — all 1000 readable with correct content")
    void clean_close_then_reopen_recovers_all() throws IOException {
        List<byte[]> originals = new ArrayList<>();

        // SESSION 1 — write 1000 messages
        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            for (int i = 0; i < 1000; i++) {
                byte[] msg = ("message-" + i + "-" + "x".repeat(i % 50)).getBytes();
                originals.add(msg);
                mgr.append("orders", msg);
            }
            assertEquals(1000L, mgr.getEndOffset("orders"));
        }

        // Verify multiple segment files exist (we forced rolls)
        long segCount = Files.list(tempDir.resolve("orders"))
                .filter(p -> p.toString().endsWith(".log"))
                .count();
        assertTrue(segCount > 1, "Expected multiple segments — got " + segCount);

        // SESSION 2 — reopen on same dataDir
        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            assertTrue(mgr.topicExists("orders"));
            assertEquals(1000L, mgr.getEndOffset("orders"));

            List<byte[]> recovered = mgr.read("orders", 0L, 1500);
            assertEquals(1000, recovered.size(), "Expected to recover all 1000 messages");

            for (int i = 0; i < 1000; i++) {
                assertArrayEquals(originals.get(i), recovered.get(i),
                        "Message " + i + " differs after recovery");
            }
        }
    }

    @Test
    @DisplayName("Simulate kill -9 (don't close) — at least N-1 messages recoverable")
    void unclean_shutdown_recovers_clean_records() throws IOException {
        // Don't use try-with-resources — simulate a crash
        LogManager mgr = new LogManager(smallSegmentsConfig());
        for (int i = 0; i < 500; i++) {
            mgr.append("orders", ("msg-" + i).getBytes());
        }
        // Skip mgr.close() — simulates the broker dying without flushing.
        // The OS page cache should still flush most data, but the very last
        // record might be torn.

        // Reopen
        try (LogManager mgr2 = new LogManager(smallSegmentsConfig())) {
            long endOffset = mgr2.getEndOffset("orders");
            assertTrue(endOffset >= 499 && endOffset <= 500,
                    "Expected end offset 499 or 500, got " + endOffset);

            List<byte[]> recovered = mgr2.read("orders", 0L, 600);
            assertTrue(recovered.size() >= 499);

            // Every recovered record should match what we wrote
            for (int i = 0; i < recovered.size(); i++) {
                assertEquals("msg-" + i, new String(recovered.get(i)));
            }
        }
    }

    @Test
    @DisplayName("Manually corrupt last few bytes — recovery truncates and end offset adjusts")
    void truncated_final_bytes_recovered_cleanly() throws IOException {
        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            for (int i = 0; i < 100; i++) {
                mgr.append("orders", ("msg-" + i).getBytes());
            }
        }

        // Find the most recent segment file
        Path topicDir = tempDir.resolve("orders");
        Path latestSegment = Files.list(topicDir)
                .filter(p -> p.toString().endsWith(".log"))
                .sorted()
                .reduce((a, b) -> b)
                .orElseThrow();

        long sizeBefore = Files.size(latestSegment);

        // Truncate the segment by 5 bytes (simulates a torn write)
        try (FileChannel ch = FileChannel.open(latestSegment, StandardOpenOption.WRITE)) {
            ch.truncate(sizeBefore - 5);
        }

        // Reopen — should detect the corruption, truncate, and continue
        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            long endOffset = mgr.getEndOffset("orders");
            // We lost the last record (or nothing, if the corruption was just slack)
            assertTrue(endOffset >= 99 && endOffset <= 100,
                    "Expected 99 or 100, got " + endOffset);

            // All recovered messages should be valid
            List<byte[]> recovered = mgr.read("orders", 0L, 200);
            for (int i = 0; i < recovered.size(); i++) {
                assertEquals("msg-" + i, new String(recovered.get(i)));
            }
        }
    }

    @Test
    @DisplayName("Multiple topics all recover correctly")
    void multipleTopics_recoverIndependently() throws IOException {
        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            for (int i = 0; i < 50; i++) mgr.append("orders", ("o-" + i).getBytes());
            for (int i = 0; i < 30; i++) mgr.append("payments", ("p-" + i).getBytes());
            for (int i = 0; i < 20; i++) mgr.append("logs", ("l-" + i).getBytes());
        }

        try (LogManager mgr = new LogManager(smallSegmentsConfig())) {
            assertEquals(50L, mgr.getEndOffset("orders"));
            assertEquals(30L, mgr.getEndOffset("payments"));
            assertEquals(20L, mgr.getEndOffset("logs"));

            assertEquals(50, mgr.read("orders", 0L, 100).size());
            assertEquals(30, mgr.read("payments", 0L, 100).size());
            assertEquals(20, mgr.read("logs", 0L, 100).size());
        }
    }
}
