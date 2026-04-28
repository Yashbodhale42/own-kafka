package com.ownkafka.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ============================================================================
 * OffsetIndex — Sparse index file for fast offset-to-position lookup.
 * ============================================================================
 *
 * WHAT: Maps "offset → byte position in the .log file" for some sample of
 *       offsets. Given a target offset, returns the closest indexed entry
 *       so the segment can seek there and scan forward (instead of scanning
 *       from byte 0).
 *
 * WHY SPARSE? Why not index every record?
 *       Imagine 10 million records. A full index would be 80 MB just for
 *       lookups, which would itself slow things down (cache misses).
 *       A sparse index (every 4 KB of log) is tiny: 80 MB log → 20 KB index.
 *       Combined with a short linear scan after the lookup, this gives
 *       near-constant-time lookups with minimal memory.
 *
 *       This is the same pattern databases use (B-tree leaves, LSM SSTables).
 *
 * INDEX FILE FORMAT (8 bytes per entry):
 *   ┌──────────────────┬──────────────────┐
 *   │ relativeOffset:4 │ filePosition:4   │
 *   └──────────────────┴──────────────────┘
 *
 *   relativeOffset = absoluteOffset - segmentBaseOffset
 *     (lets us use 4 bytes instead of 8 — segments are < 2 GB)
 *
 *   filePosition = byte offset in the .log file where this record starts
 *
 * IN-MEMORY STRUCTURE:
 *   We hold the entries as parallel arrays: long[] offsets, int[] positions.
 *   Why arrays? Java's binary search on arrays is incredibly fast and
 *   cache-friendly. No object overhead per entry.
 *
 * THREAD SAFETY:
 *   - One writer (the active segment's append path) calls maybeAppend()
 *   - Many readers (fetch handlers) call lookup() concurrently
 *   - Solution: ReentrantReadWriteLock — many readers can hold the read
 *     lock at once; writers get exclusive access via the write lock.
 *
 * INTERVIEW TIP: "How does Kafka find a message by offset?"
 *       → Sparse offset index + binary search + sequential scan.
 *         The index has one entry per ~4 KB of log. Find the floor entry
 *         via binary search, seek to that file position, scan forward
 *         until you reach the requested offset. Effectively O(log N)
 *         with bounded scan distance.
 * ============================================================================
 */
final class OffsetIndex implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(OffsetIndex.class);

    /** Each entry on disk is 8 bytes: 4-byte relativeOffset + 4-byte filePosition. */
    static final int ENTRY_SIZE = 8;

    /** Initial capacity for the in-memory arrays — grow as needed. */
    private static final int INITIAL_CAPACITY = 256;

    private final Path indexFile;
    private final long baseOffset;
    private final FileChannel channel;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Parallel arrays: offsets[i] and positions[i] form one entry. */
    private long[] offsets;
    private int[] positions;

    /** How many entries are currently used (rest of arrays is unused capacity). */
    private int entryCount;

    /**
     * Open or create an index file.
     *
     * @param indexFile    path to the .index file
     * @param baseOffset   the segment's base offset (offset of its first record)
     */
    OffsetIndex(Path indexFile, long baseOffset) throws IOException {
        this.indexFile = indexFile;
        this.baseOffset = baseOffset;

        // Open with READ + WRITE + CREATE. APPEND would prevent us from truncating
        // (which we need for recovery of torn entries).
        this.channel = FileChannel.open(
                indexFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        this.offsets = new long[INITIAL_CAPACITY];
        this.positions = new int[INITIAL_CAPACITY];
        this.entryCount = 0;

        load();
    }

    /**
     * Loads existing entries from the file into memory.
     * Validates that the file size is a multiple of 8 bytes;
     * if not, the trailing partial entry is truncated.
     */
    public void load() throws IOException {
        lock.writeLock().lock();
        try {
            long fileSize = channel.size();

            // If file size isn't a clean multiple of ENTRY_SIZE, the last entry
            // is torn. Truncate it.
            long cleanSize = (fileSize / ENTRY_SIZE) * ENTRY_SIZE;
            if (cleanSize != fileSize) {
                logger.warn("Index file {} has trailing partial entry ({} extra bytes), truncating",
                        indexFile, fileSize - cleanSize);
                channel.truncate(cleanSize);
                fileSize = cleanSize;
            }

            int numEntries = (int) (fileSize / ENTRY_SIZE);

            // Grow in-memory arrays if needed
            if (numEntries > offsets.length) {
                grow(numEntries);
            }

            // Read all entries in one buffer for speed
            ByteBuffer buf = ByteBuffer.allocate((int) fileSize);
            channel.read(buf, 0);
            buf.flip();

            for (int i = 0; i < numEntries; i++) {
                int relativeOffset = buf.getInt();
                int position = buf.getInt();
                offsets[i] = baseOffset + relativeOffset;
                positions[i] = position;
            }
            entryCount = numEntries;

            // Position the channel at end so subsequent appends go there
            channel.position(fileSize);

            logger.debug("Loaded {} index entries from {}", entryCount, indexFile);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append a new index entry: "record at absoluteOffset starts at filePosition in the .log".
     * Should be called by the segment whenever indexIntervalBytes have been written
     * since the last entry.
     */
    public void maybeAppend(long absoluteOffset, int filePosition) throws IOException {
        lock.writeLock().lock();
        try {
            // Sanity: entries must be monotonically increasing in offset
            if (entryCount > 0 && absoluteOffset <= offsets[entryCount - 1]) {
                throw new IllegalArgumentException(
                        "Offsets must be monotonically increasing: last="
                                + offsets[entryCount - 1] + ", new=" + absoluteOffset);
            }

            // Grow the arrays if needed
            if (entryCount >= offsets.length) {
                grow(offsets.length * 2);
            }

            offsets[entryCount] = absoluteOffset;
            positions[entryCount] = filePosition;
            entryCount++;

            // Persist to disk
            ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
            int relativeOffset = (int) (absoluteOffset - baseOffset);
            buf.putInt(relativeOffset);
            buf.putInt(filePosition);
            buf.flip();
            while (buf.hasRemaining()) {
                channel.write(buf);
            }

            logger.debug("Index entry: offset {} → position {}", absoluteOffset, filePosition);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Find the largest entry whose offset is <= targetOffset.
     * Returns null if no such entry exists (target precedes the first entry).
     *
     * This is "floor lookup" — like Java's TreeMap.floorEntry().
     * Used by Segment.read() to figure out where to start scanning.
     */
    public IndexEntry lookup(long targetOffset) {
        lock.readLock().lock();
        try {
            if (entryCount == 0) {
                return null;
            }

            // Binary search for the largest offset <= targetOffset
            int low = 0;
            int high = entryCount - 1;

            // Quick exits
            if (targetOffset < offsets[low]) {
                return null;  // target precedes first entry
            }
            if (targetOffset >= offsets[high]) {
                return new IndexEntry(offsets[high], positions[high]);
            }

            // Standard binary search for floor
            while (low < high) {
                int mid = (low + high + 1) / 2;
                if (offsets[mid] <= targetOffset) {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }

            return new IndexEntry(offsets[low], positions[low]);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int entryCount() {
        lock.readLock().lock();
        try {
            return entryCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Truncate the index to remove all entries with position >= maxPosition.
     * Used during segment recovery when the .log file gets truncated.
     */
    public void truncateAfter(int maxPosition) throws IOException {
        lock.writeLock().lock();
        try {
            // Find the first entry with position > maxPosition
            int newCount = entryCount;
            for (int i = 0; i < entryCount; i++) {
                if (positions[i] > maxPosition) {
                    newCount = i;
                    break;
                }
            }

            if (newCount < entryCount) {
                int removed = entryCount - newCount;
                entryCount = newCount;
                long newFileSize = (long) newCount * ENTRY_SIZE;
                channel.truncate(newFileSize);
                channel.position(newFileSize);
                logger.info("Truncated {} index entries beyond position {}", removed, maxPosition);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void flush() throws IOException {
        // force(true) = sync data + metadata to disk
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Delete the index file from disk. Must be called only after close().
     */
    public void delete() throws IOException {
        Files.deleteIfExists(indexFile);
    }

    private void grow(int minCapacity) {
        int newCapacity = Math.max(minCapacity, offsets.length * 2);
        long[] newOffsets = new long[newCapacity];
        int[] newPositions = new int[newCapacity];
        System.arraycopy(offsets, 0, newOffsets, 0, entryCount);
        System.arraycopy(positions, 0, newPositions, 0, entryCount);
        offsets = newOffsets;
        positions = newPositions;
    }

    /**
     * One index entry — pairs an absolute offset with its byte position in the log file.
     */
    public record IndexEntry(long absoluteOffset, int filePosition) {}
}
