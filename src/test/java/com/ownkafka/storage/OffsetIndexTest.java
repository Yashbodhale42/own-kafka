package com.ownkafka.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for OffsetIndex — verifies sparse offset → file position lookup.
 */
class OffsetIndexTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Empty index lookup returns null")
    void emptyIndex_lookup_returnsNull() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            assertNull(index.lookup(100L));
            assertEquals(0, index.entryCount());
        }
    }

    @Test
    @DisplayName("Append + lookup returns the same entry")
    void append_thenLookup_exactMatch() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(10L, 100);
            index.maybeAppend(20L, 200);
            index.maybeAppend(30L, 300);

            assertEquals(3, index.entryCount());

            OffsetIndex.IndexEntry e1 = index.lookup(10L);
            assertEquals(10L, e1.absoluteOffset());
            assertEquals(100, e1.filePosition());

            OffsetIndex.IndexEntry e3 = index.lookup(30L);
            assertEquals(30L, e3.absoluteOffset());
            assertEquals(300, e3.filePosition());
        }
    }

    @Test
    @DisplayName("Lookup below first entry returns null")
    void lookupBelowFirst_returnsNull() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(50L, 500);
            assertNull(index.lookup(10L));
        }
    }

    @Test
    @DisplayName("Lookup between entries returns the floor (closest below)")
    void lookupBetween_returnsFloor() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(10L, 100);
            index.maybeAppend(20L, 200);
            index.maybeAppend(30L, 300);

            OffsetIndex.IndexEntry e = index.lookup(25L);
            assertEquals(20L, e.absoluteOffset());
            assertEquals(200, e.filePosition());
        }
    }

    @Test
    @DisplayName("Lookup above last entry returns the last entry")
    void lookupAboveLast_returnsLast() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(10L, 100);
            index.maybeAppend(20L, 200);

            OffsetIndex.IndexEntry e = index.lookup(999L);
            assertEquals(20L, e.absoluteOffset());
        }
    }

    @Test
    @DisplayName("Reopen loads all entries from disk")
    void reopen_loadsEntries() throws IOException {
        Path indexFile = tempDir.resolve("00.index");

        try (OffsetIndex index = new OffsetIndex(indexFile, 0L)) {
            for (int i = 0; i < 10; i++) {
                index.maybeAppend(i * 10, i * 100);
            }
            assertEquals(10, index.entryCount());
        }

        // Reopen — should see all 10 entries
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L)) {
            assertEquals(10, reopened.entryCount());

            OffsetIndex.IndexEntry e = reopened.lookup(50L);
            assertEquals(50L, e.absoluteOffset());
            assertEquals(500, e.filePosition());
        }
    }

    @Test
    @DisplayName("Trailing partial entry is truncated on reopen")
    void partialTrailingEntry_truncated() throws IOException {
        Path indexFile = tempDir.resolve("00.index");

        try (OffsetIndex index = new OffsetIndex(indexFile, 0L)) {
            index.maybeAppend(10L, 100);
            index.maybeAppend(20L, 200);
        }

        // Manually corrupt: add 3 stray bytes at the end
        Files.write(indexFile, new byte[]{0x01, 0x02, 0x03},
                java.nio.file.StandardOpenOption.APPEND);

        // Reopen should detect and fix
        try (OffsetIndex reopened = new OffsetIndex(indexFile, 0L)) {
            assertEquals(2, reopened.entryCount());
            assertEquals(16, Files.size(indexFile));
        }
    }

    @Test
    @DisplayName("Non-monotonic append throws")
    void nonMonotonicAppend_throws() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(20L, 200);
            assertThrows(IllegalArgumentException.class, () -> index.maybeAppend(10L, 100));
        }
    }

    @Test
    @DisplayName("baseOffset is preserved")
    void baseOffsetPreserved() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 5000L)) {
            assertEquals(5000L, index.baseOffset());
        }
    }

    @Test
    @DisplayName("truncateAfter removes entries beyond a position")
    void truncateAfter_removesEntries() throws IOException {
        try (OffsetIndex index = new OffsetIndex(tempDir.resolve("00.index"), 0L)) {
            index.maybeAppend(10L, 100);
            index.maybeAppend(20L, 200);
            index.maybeAppend(30L, 300);
            index.maybeAppend(40L, 400);

            // Truncate everything past position 250
            index.truncateAfter(250);

            // Only entries with position <= 250 should remain (10→100, 20→200)
            assertEquals(2, index.entryCount());
            assertEquals(20L, index.lookup(999L).absoluteOffset());
        }
    }
}
