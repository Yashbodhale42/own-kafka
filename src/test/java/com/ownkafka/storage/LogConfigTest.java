package com.ownkafka.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LogConfig — configuration record validation.
 */
class LogConfigTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("defaults() produces sane production values")
    void defaults_isSane() {
        LogConfig cfg = LogConfig.defaults();
        assertNotNull(cfg.dataDir());
        assertTrue(cfg.segmentSizeBytes() > 0);
        assertTrue(cfg.segmentSizeBytes() < Integer.MAX_VALUE);
        assertTrue(cfg.retentionBytes() > 0);
        assertTrue(cfg.retentionMillis() > 0);
        assertTrue(cfg.indexIntervalBytes() > 0);
    }

    @Test
    @DisplayName("forTesting() produces tiny values")
    void forTesting_isTiny() {
        LogConfig cfg = LogConfig.forTesting(tempDir);
        assertEquals(tempDir, cfg.dataDir());
        assertTrue(cfg.segmentSizeBytes() < 10_000);
    }

    @Test
    @DisplayName("Compact constructor rejects negative segment size")
    void rejectsNegativeSegmentSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new LogConfig(tempDir, -1L, 100, 1000L, 32));
    }

    @Test
    @DisplayName("Compact constructor rejects null dataDir")
    void rejectsNullDataDir() {
        assertThrows(IllegalArgumentException.class, () ->
                new LogConfig(null, 1024, 100, 1000L, 32));
    }

    @Test
    @DisplayName("Compact constructor rejects segment size >= 2 GB (int overflow guard)")
    void rejectsHugeSegmentSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new LogConfig(tempDir, Integer.MAX_VALUE, 100, 1000L, 32));
    }

    @Test
    @DisplayName("Compact constructor rejects zero retention")
    void rejectsZeroRetention() {
        assertThrows(IllegalArgumentException.class, () ->
                new LogConfig(tempDir, 1024, 0, 1000L, 32));
    }
}
