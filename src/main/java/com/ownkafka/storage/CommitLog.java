package com.ownkafka.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * ============================================================================
 * CommitLog — Manages all segments for ONE topic.
 * ============================================================================
 *
 * WHAT: A topic's data on disk is a sequence of segments. CommitLog is the
 *       class that knows about the whole sequence:
 *       - Holds segments in offset-order
 *       - Routes reads to the right segment (or across multiple segments)
 *       - Rolls a new segment when the active one fills up
 *       - Enforces retention (deletes old segments)
 *       - Recovers all segments from disk on startup
 *
 * STRUCTURE:
 *   data/orders/
 *     ├── 00000000000000000000.log    ← oldest segment (read-only)
 *     ├── 00000000000000000000.index
 *     ├── 00000000000000050000.log    ← rolled segment
 *     ├── 00000000000000050000.index
 *     ├── 00000000000000100000.log    ← active segment (being written)
 *     └── 00000000000000100000.index
 *
 *   The HIGHEST baseOffset segment is the "active" segment.
 *   That's the only one we append to; others are read-only.
 *
 * APPEND PATH:
 *   1. Take the appendLock (single writer per topic)
 *   2. Check if active segment is full → if yes, roll
 *   3. Delegate append to active segment
 *
 * ROLL PROCESS:
 *   1. Flush + close the old active segment
 *   2. Compute new baseOffset = oldActive.nextOffset()
 *   3. Create a new Segment with that baseOffset
 *   4. Insert into the segment map; the highest entry is now the new active
 *
 * READ PATH (can span segments):
 *   1. Find the segment containing fromOffset (floorEntry on the map)
 *   2. Read from that segment
 *   3. If we got fewer records than requested, continue into the next segment
 *
 * RETENTION:
 *   Walk the segments oldest-to-newest. Delete an "old" segment if EITHER:
 *   - total topic size > retentionBytes, OR
 *   - segment age > retentionMillis
 *   NEVER delete the active segment (it's the write head).
 *   Always keep at least one segment.
 *
 * THREAD SAFETY:
 *   - segments: ConcurrentSkipListMap (lock-free reads, atomic operations)
 *   - appends: serialized by appendLock (one ReentrantLock per topic)
 *   - reads: lock-free at this level; segment-level locks handle concurrency
 *
 * INTERVIEW TIP: "How does Kafka delete old data?"
 *       → Time and size-based retention. Periodically (5-min intervals by
 *         default) Kafka checks each topic's segments and deletes whole
 *         segments that exceed retention.bytes or are older than
 *         retention.ms. The active segment is never deleted. This is
 *         O(1) per deletion — just unlink a file — vs O(N) record-by-record.
 * ============================================================================
 */
final class CommitLog implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private final String topic;
    private final Path topicDir;
    private final LogConfig config;
    private final ReentrantLock appendLock = new ReentrantLock();

    /** Segments keyed by baseOffset. Last entry is the active segment. */
    private final ConcurrentSkipListMap<Long, Segment> segments = new ConcurrentSkipListMap<>();

    /**
     * Open or create a topic's commit log.
     * Scans the topic directory, recovers any existing segments,
     * and creates an initial empty segment if the directory is empty.
     */
    static CommitLog openOrCreate(String topic, LogConfig config) throws IOException {
        Path topicDir = config.dataDir().resolve(topic);
        Files.createDirectories(topicDir);

        CommitLog log = new CommitLog(topic, topicDir, config);
        log.recoverAllSegments();

        // If no segments exist yet, create an empty initial segment at offset 0
        if (log.segments.isEmpty()) {
            Segment initial = Segment.openOrCreate(topicDir, 0L, config);
            log.segments.put(0L, initial);
            logger.info("Created initial empty segment for topic '{}'", topic);
        }

        return log;
    }

    private CommitLog(String topic, Path topicDir, LogConfig config) {
        this.topic = topic;
        this.topicDir = topicDir;
        this.config = config;
    }

    /**
     * Scan the topic directory for existing segment files and reopen each.
     * Filenames are 20-digit zero-padded base offsets.
     */
    private void recoverAllSegments() throws IOException {
        try (Stream<Path> stream = Files.list(topicDir)) {
            stream
                    .filter(p -> p.toString().endsWith(".log"))
                    .sorted()
                    .forEach(logFile -> {
                        String filename = logFile.getFileName().toString();
                        String baseOffsetStr = filename.substring(0, filename.length() - ".log".length());
                        try {
                            long baseOffset = Long.parseLong(baseOffsetStr);
                            Segment segment = Segment.openOrCreate(topicDir, baseOffset, config);
                            segments.put(baseOffset, segment);
                            logger.debug("Recovered segment {} for topic '{}'", baseOffset, topic);
                        } catch (NumberFormatException e) {
                            logger.warn("Skipping non-segment file: {}", filename);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to recover segment " + filename, e);
                        }
                    });
        }
    }

    public String topic() {
        return topic;
    }

    /** The offset that would be assigned to the next appended record. */
    public long endOffset() {
        Map.Entry<Long, Segment> active = segments.lastEntry();
        return (active != null) ? active.getValue().nextOffset() : 0L;
    }

    /** The smallest offset still available (oldest segment's baseOffset). */
    public long startOffset() {
        Map.Entry<Long, Segment> oldest = segments.firstEntry();
        return (oldest != null) ? oldest.getValue().baseOffset() : 0L;
    }

    public long totalSizeBytes() {
        return segments.values().stream().mapToLong(Segment::sizeBytes).sum();
    }

    /**
     * Append a record. Returns the assigned offset.
     * Rolls a new segment if the active one is full.
     */
    public long append(byte[] value) throws IOException {
        appendLock.lock();
        try {
            Segment active = segments.lastEntry().getValue();

            // Roll if full
            if (active.isFull(config.segmentSizeBytes())) {
                active = rollSegment();
            }

            LogRecord record = active.append(value, System.currentTimeMillis());
            return record.offset();
        } finally {
            appendLock.unlock();
        }
    }

    /** Roll a new active segment. Caller must hold appendLock. */
    private Segment rollSegment() throws IOException {
        Segment oldActive = segments.lastEntry().getValue();
        long newBaseOffset = oldActive.nextOffset();

        // Flush the old one (it becomes read-only after this point)
        oldActive.flush();

        Segment newActive = Segment.openOrCreate(topicDir, newBaseOffset, config);
        segments.put(newBaseOffset, newActive);

        logger.info("Rolled topic '{}' to new segment at base offset {}", topic, newBaseOffset);
        return newActive;
    }

    /**
     * Read up to maxRecords records starting at fromOffset.
     * Reads can span multiple segments.
     */
    public List<LogRecord> read(long fromOffset, int maxRecords, int maxBytes) throws IOException {
        if (fromOffset >= endOffset()) {
            return List.of();
        }

        // Clamp to startOffset if requesting older than we have
        long actualFrom = Math.max(fromOffset, startOffset());

        List<LogRecord> result = new ArrayList<>();
        int remainingRecords = maxRecords;
        int remainingBytes = maxBytes;

        // Find the segment containing actualFrom
        Map.Entry<Long, Segment> entry = segments.floorEntry(actualFrom);
        if (entry == null) {
            entry = segments.firstEntry();
        }

        // Walk segments forward, collecting records until we hit limits
        while (entry != null && remainingRecords > 0 && remainingBytes > 0) {
            Segment segment = entry.getValue();
            List<LogRecord> chunk = segment.read(actualFrom, remainingRecords, remainingBytes);

            for (LogRecord r : chunk) {
                result.add(r);
                remainingRecords--;
                remainingBytes -= r.wireSize();
                actualFrom = r.offset() + 1;
                if (remainingRecords <= 0 || remainingBytes <= 0) break;
            }

            if (chunk.isEmpty()) {
                // No more records in this segment — try the next one
                entry = segments.higherEntry(entry.getKey());
            } else if (remainingRecords > 0 && remainingBytes > 0) {
                // We got some, but want more — move to next segment
                entry = segments.higherEntry(entry.getKey());
            }
        }

        return result;
    }

    /**
     * Apply size and age retention. Returns the number of segments deleted.
     * Never deletes the active segment. Always keeps at least one segment.
     */
    public int enforceRetention() throws IOException {
        appendLock.lock();
        try {
            int deleted = 0;
            long now = System.currentTimeMillis();

            while (segments.size() > 1) {
                Map.Entry<Long, Segment> oldestEntry = segments.firstEntry();
                Segment oldest = oldestEntry.getValue();

                // Don't delete the active segment (last entry)
                if (oldestEntry.getKey().equals(segments.lastEntry().getKey())) {
                    break;
                }

                long totalSize = totalSizeBytes();
                long age = now - oldest.createdMillis();

                boolean overSize = totalSize > config.retentionBytes();
                boolean overAge = age > config.retentionMillis();

                if (!overSize && !overAge) {
                    break;  // oldest is within retention — stop
                }

                segments.remove(oldestEntry.getKey());
                oldest.delete();
                deleted++;
                logger.info("Deleted segment {} (overSize={}, overAge={})",
                        oldestEntry.getKey(), overSize, overAge);
            }

            return deleted;
        } finally {
            appendLock.unlock();
        }
    }

    public void flush() throws IOException {
        for (Segment segment : segments.values()) {
            segment.flush();
        }
    }

    @Override
    public void close() throws IOException {
        appendLock.lock();
        try {
            for (Segment segment : segments.values()) {
                try {
                    segment.close();
                } catch (IOException e) {
                    logger.warn("Error closing segment {}: {}", segment.baseOffset(), e.getMessage());
                }
            }
            segments.clear();
        } finally {
            appendLock.unlock();
        }
    }
}
