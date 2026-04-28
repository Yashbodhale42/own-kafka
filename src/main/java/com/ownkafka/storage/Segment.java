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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ============================================================================
 * Segment — One .log file + one .index file pair (the unit of storage).
 * ============================================================================
 *
 * WHAT: A segment is a chunk of contiguous records on disk. Each segment has:
 *       - a .log file: the actual record bytes (append-only)
 *       - a .index file: sparse offset→position map (for fast lookup)
 *       - a base offset: the offset of its very first record
 *
 *       Filename is the base offset, padded to 20 digits:
 *         00000000000000000000.log    (base offset 0)
 *         00000000000000050000.log    (base offset 50000)
 *
 * WHY 20-DIGIT PADDING?
 *       So segments sort lexicographically (alphabetically) the same as
 *       numerically. ls / Files.list() gives them back in the right order.
 *
 * APPEND PATH:
 *       1. Acquire write lock (single writer per segment).
 *       2. Compute next offset = baseOffset + recordCount.
 *       3. If we've written indexIntervalBytes since the last index entry,
 *          add an entry to the OffsetIndex first (so it points at the
 *          upcoming record's position).
 *       4. Encode the record via RecordCodec, write to channel.
 *       5. Update counters.
 *
 * READ PATH:
 *       1. Look up the closest index entry for the requested offset.
 *       2. Position a buffer-backed read at that file position.
 *       3. Decode records sequentially via RecordCodec.readFromBuffer.
 *       4. Skip until offset >= requested, then collect maxRecords / maxBytes.
 *
 * RECOVERY:
 *       On open, scan the .log file from byte 0:
 *       - Read records one by one
 *       - If we hit CorruptRecordException, truncate the log file there
 *       - Also truncate the index to drop entries beyond that point
 *       This handles "kill -9 mid-write" gracefully.
 *
 * THREAD SAFETY:
 *       - One writer + many readers
 *       - ReentrantReadWriteLock: writers exclusive, readers concurrent
 *       - Reads use POSITIONAL channel.read(buf, position) which does NOT
 *         move the shared channel position — safe for concurrent readers.
 *
 * WINDOWS NOTE:
 *       We use FileChannel, NOT MappedByteBuffer. mmap on Windows holds the
 *       file open until GC, which prevents Files.delete() from succeeding.
 *       Plain FileChannel releases the handle as soon as close() is called.
 *
 * INTERVIEW TIP: "Why does Kafka split a topic's log into segments?"
 *       → Three reasons:
 *         1. Retention — you can delete an old segment as a single file.
 *         2. Recovery — only the active segment needs scanning on startup.
 *         3. Index size — each segment has its own bounded index.
 * ============================================================================
 */
final class Segment implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Segment.class);

    /** Length of the base-offset filename prefix: 20 digits zero-padded. */
    private static final int FILENAME_DIGITS = 20;

    private final long baseOffset;
    private final Path logFile;
    private final Path indexFilePath;
    private final FileChannel logChannel;
    private final OffsetIndex index;
    private final LogConfig config;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final long createdMillis;

    /** How many records this segment currently holds. */
    private final AtomicLong recordCount = new AtomicLong(0);

    /** Bytes written since the last index entry (to know when to write the next one). */
    private long bytesSinceLastIndexEntry = 0;

    /**
     * Open or create a segment. If the .log file exists, it will be scanned and
     * recovered (torn records truncated). If not, a fresh empty segment is created.
     */
    static Segment openOrCreate(Path topicDir, long baseOffset, LogConfig config) throws IOException {
        Files.createDirectories(topicDir);

        String filename = String.format("%0" + FILENAME_DIGITS + "d", baseOffset);
        Path logFile = topicDir.resolve(filename + ".log");
        Path indexFile = topicDir.resolve(filename + ".index");

        boolean isNewSegment = !Files.exists(logFile);

        Segment segment = new Segment(baseOffset, logFile, indexFile, config);

        if (!isNewSegment) {
            segment.recover();
        }

        return segment;
    }

    private Segment(long baseOffset, Path logFile, Path indexFile, LogConfig config) throws IOException {
        this.baseOffset = baseOffset;
        this.logFile = logFile;
        this.indexFilePath = indexFile;
        this.config = config;

        this.logChannel = FileChannel.open(
                logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        // Position channel at end so writes append
        logChannel.position(logChannel.size());

        this.index = new OffsetIndex(indexFile, baseOffset);

        // Use file's last-modified time as a proxy for "when was this segment created"
        // (good enough for retention purposes; real Kafka uses message timestamps)
        this.createdMillis = Files.exists(logFile)
                ? Files.getLastModifiedTime(logFile).toMillis()
                : System.currentTimeMillis();
    }

    public long baseOffset() {
        return baseOffset;
    }

    /** The offset that would be assigned to the NEXT record appended. */
    public long nextOffset() {
        return baseOffset + recordCount.get();
    }

    /** Current size of the .log file in bytes. */
    public long sizeBytes() {
        try {
            return logChannel.size();
        } catch (IOException e) {
            return 0;
        }
    }

    public long createdMillis() {
        return createdMillis;
    }

    /** True if this segment has reached the size limit and should roll. */
    public boolean isFull(long segmentSizeBytes) {
        return sizeBytes() >= segmentSizeBytes;
    }

    /**
     * Append a record. Returns the assigned LogRecord.
     * Single-writer design — only the segment's owner (CommitLog) calls this,
     * and CommitLog serializes all appends with its own lock.
     */
    public LogRecord append(byte[] value, long timestampMs) throws IOException {
        lock.writeLock().lock();
        try {
            long offset = nextOffset();
            long currentPosition = logChannel.position();

            // Maybe write an index entry FIRST — it must point at the position
            // BEFORE we write the upcoming record. That way looking up this
            // offset later seeks to the start of the record.
            if (bytesSinceLastIndexEntry >= config.indexIntervalBytes() || recordCount.get() == 0) {
                index.maybeAppend(offset, (int) currentPosition);
                bytesSinceLastIndexEntry = 0;
            }

            LogRecord record = new LogRecord(offset, timestampMs, value);
            int bytesWritten = RecordCodec.writeTo(logChannel, record);

            recordCount.incrementAndGet();
            bytesSinceLastIndexEntry += bytesWritten;

            return record;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Read up to maxRecords records starting at fromOffset (inclusive).
     * Returns empty list if fromOffset is at or beyond the end.
     *
     * Thread-safe: uses positional reads, holds read lock.
     */
    public List<LogRecord> read(long fromOffset, int maxRecords, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            long endOffset = nextOffset();
            if (fromOffset >= endOffset) {
                return List.of();
            }
            if (fromOffset < baseOffset) {
                fromOffset = baseOffset;
            }

            // Step 1: find the closest indexed position
            OffsetIndex.IndexEntry entry = index.lookup(fromOffset);
            long startPosition = (entry != null) ? entry.filePosition() : 0L;

            // Step 2: read a chunk of bytes starting from startPosition
            // We read up to maxBytes (or the file end, whichever is smaller).
            long fileSize = logChannel.size();
            int chunkSize = (int) Math.min(maxBytes, fileSize - startPosition);
            if (chunkSize <= 0) {
                return List.of();
            }

            ByteBuffer chunk = ByteBuffer.allocate(chunkSize);
            int totalRead = 0;
            long pos = startPosition;
            while (chunk.hasRemaining() && totalRead < chunkSize) {
                int read = logChannel.read(chunk, pos);
                if (read == -1) break;
                totalRead += read;
                pos += read;
            }
            chunk.flip();

            // Step 3: decode records, skip until fromOffset, then collect
            List<LogRecord> records = new ArrayList<>();
            while (chunk.hasRemaining() && records.size() < maxRecords) {
                LogRecord record;
                try {
                    record = RecordCodec.readFromBuffer(chunk);
                } catch (CorruptRecordException e) {
                    // Hit end of valid data in this chunk — that's ok, just stop.
                    break;
                }

                if (record.offset() < fromOffset) {
                    continue;  // Skip records before our requested offset
                }
                if (record.offset() >= endOffset) {
                    break;  // Past the end (defensive)
                }

                records.add(record);
            }

            return records;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Recovery: scan the .log file, count valid records, truncate any torn record.
     * Called on startup if the segment file already exists.
     *
     * Returns the number of valid records found.
     */
    public long recover() throws IOException {
        lock.writeLock().lock();
        try {
            long fileSize = logChannel.size();
            long position = 0L;
            long count = 0;
            long lastValidPosition = 0L;

            // Scan from the beginning, decoding each record
            while (position < fileSize) {
                try {
                    LogRecord record = RecordCodec.readFrom(logChannel, position);
                    int recordSize = record.wireSize();
                    position += recordSize;
                    lastValidPosition = position;
                    count++;
                } catch (CorruptRecordException e) {
                    // Hit a torn record — truncate the file here
                    logger.warn("Segment {} has torn record at position {} (file size {}); truncating",
                            logFile, position, fileSize);
                    logChannel.truncate(lastValidPosition);
                    logChannel.position(lastValidPosition);

                    // Also truncate the index
                    index.truncateAfter((int) lastValidPosition);
                    break;
                }
            }

            recordCount.set(count);
            // Ensure channel is positioned at end for future appends
            logChannel.position(logChannel.size());

            // Reset index counter — we just recovered, the next append will
            // trigger a fresh index entry.
            bytesSinceLastIndexEntry = 0;

            logger.info("Recovered segment {} with {} records, size={} bytes",
                    logFile.getFileName(), count, logChannel.size());
            return count;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Force data + metadata to disk (fsync). */
    public void flush() throws IOException {
        lock.readLock().lock();
        try {
            if (logChannel.isOpen()) {
                logChannel.force(true);
            }
            index.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (logChannel.isOpen()) {
                logChannel.force(true);
                logChannel.close();
            }
            index.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Close + delete both files. Used by retention. */
    public void delete() throws IOException {
        close();
        Files.deleteIfExists(logFile);
        index.delete();
        logger.info("Deleted segment {}", logFile.getFileName());
    }
}
