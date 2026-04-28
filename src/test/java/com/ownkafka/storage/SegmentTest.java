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
 * Tests for Segment — the unit of disk storage (one .log + one .index).
 */
class SegmentTest {

    @TempDir
    Path tempDir;

    private LogConfig config() {
        return LogConfig.forTesting(tempDir);
    }

    @Test
    @DisplayName("Append assigns sequential offsets starting from base offset")
    void appendsSequentialOffsets() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            LogRecord r0 = segment.append("first".getBytes(), System.currentTimeMillis());
            LogRecord r1 = segment.append("second".getBytes(), System.currentTimeMillis());
            LogRecord r2 = segment.append("third".getBytes(), System.currentTimeMillis());

            assertEquals(0L, r0.offset());
            assertEquals(1L, r1.offset());
            assertEquals(2L, r2.offset());

            assertEquals(3L, segment.nextOffset());
        }
    }

    @Test
    @DisplayName("Read from beginning returns all records")
    void readFromBeginning() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            segment.append("a".getBytes(), 1L);
            segment.append("b".getBytes(), 2L);
            segment.append("c".getBytes(), 3L);

            List<LogRecord> records = segment.read(0L, 10, 1024);
            assertEquals(3, records.size());
            assertEquals("a", new String(records.get(0).value()));
            assertEquals("b", new String(records.get(1).value()));
            assertEquals("c", new String(records.get(2).value()));
        }
    }

    @Test
    @DisplayName("Read from middle offset skips earlier records")
    void readFromMiddle() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            for (int i = 0; i < 10; i++) {
                segment.append(("msg-" + i).getBytes(), 0L);
            }

            List<LogRecord> records = segment.read(5L, 100, 1024);
            assertEquals(5, records.size());
            assertEquals(5L, records.get(0).offset());
            assertEquals("msg-5", new String(records.get(0).value()));
        }
    }

    @Test
    @DisplayName("Read past end returns empty list")
    void readPastEnd_empty() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            segment.append("only".getBytes(), 0L);
            assertTrue(segment.read(99L, 10, 1024).isEmpty());
        }
    }

    @Test
    @DisplayName("Read respects maxRecords limit")
    void readMaxRecords() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            for (int i = 0; i < 10; i++) {
                segment.append(("msg-" + i).getBytes(), 0L);
            }
            assertEquals(3, segment.read(0L, 3, 4096).size());
        }
    }

    @Test
    @DisplayName("isFull returns true when sizeBytes >= limit")
    void isFull() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            assertFalse(segment.isFull(1000L));

            // Append until we exceed 100 bytes
            for (int i = 0; i < 20; i++) {
                segment.append("xxxxxxxxxx".getBytes(), 0L); // 30 bytes per record (20 header + 10)
            }

            assertTrue(segment.isFull(100L));
        }
    }

    @Test
    @DisplayName("Reopen recovers all records")
    void reopen_recoversRecords() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 0L, config())) {
            for (int i = 0; i < 5; i++) {
                segment.append(("msg-" + i).getBytes(), 0L);
            }
        }

        // Reopen — should recover all 5
        try (Segment reopened = Segment.openOrCreate(tempDir, 0L, config())) {
            assertEquals(5L, reopened.nextOffset());
            List<LogRecord> records = reopened.read(0L, 10, 1024);
            assertEquals(5, records.size());
        }
    }

    @Test
    @DisplayName("Recovery truncates a torn record at the end")
    void recovery_truncatesTornRecord() throws IOException {
        Path topicDir = tempDir;
        try (Segment segment = Segment.openOrCreate(topicDir, 0L, config())) {
            for (int i = 0; i < 5; i++) {
                segment.append(("msg-" + i).getBytes(), 0L);
            }
        }

        // Manually corrupt: append a few junk bytes (simulating a torn write)
        Path logFile = topicDir.resolve("00000000000000000000.log");
        long originalSize = Files.size(logFile);
        Files.write(logFile, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
                java.nio.file.StandardOpenOption.APPEND);

        // Reopen — recovery should detect torn record and truncate
        try (Segment reopened = Segment.openOrCreate(topicDir, 0L, config())) {
            // The original data should still be readable
            List<LogRecord> records = reopened.read(0L, 10, 1024);
            assertEquals(5, records.size());
            // File size should be back to original
            assertEquals(originalSize, Files.size(logFile));
        }
    }

    @Test
    @DisplayName("Segment with non-zero base offset")
    void nonZeroBaseOffset() throws IOException {
        try (Segment segment = Segment.openOrCreate(tempDir, 1000L, config())) {
            LogRecord r = segment.append("hi".getBytes(), 0L);
            assertEquals(1000L, r.offset());
            assertEquals(1001L, segment.nextOffset());
        }
    }

    @Test
    @DisplayName("delete removes both .log and .index files")
    void delete_removesBothFiles() throws IOException {
        Segment segment = Segment.openOrCreate(tempDir, 0L, config());
        segment.append("hello".getBytes(), 0L);
        segment.close();

        assertTrue(Files.exists(tempDir.resolve("00000000000000000000.log")));
        assertTrue(Files.exists(tempDir.resolve("00000000000000000000.index")));

        segment.delete();

        assertFalse(Files.exists(tempDir.resolve("00000000000000000000.log")));
        assertFalse(Files.exists(tempDir.resolve("00000000000000000000.index")));
    }
}
