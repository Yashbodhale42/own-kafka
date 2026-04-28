package com.ownkafka.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * ============================================================================
 * RecordCodec — Encode/decode LogRecord to/from disk bytes.
 * ============================================================================
 *
 * WHAT: This is the ONLY place that knows the on-disk byte format of a record.
 *       Every other class (Segment, CommitLog, etc.) deals with LogRecord
 *       objects and calls this class to serialize.
 *
 * WHY ISOLATE THE FORMAT HERE?
 *       Tomorrow we might switch to a different format (compressed, with CRC,
 *       with headers, etc.). If only one class knows the bytes, we change
 *       only one file.
 *
 *       This is the same idea as Phase 1's ProtocolCodec — separation of
 *       concerns between "what's the data" and "how is it serialized".
 *
 * ON-DISK RECORD FORMAT:
 *   ┌──────────────┬───────────────┬──────────────┬─────────────────┐
 *   │ offset (8)   │ timestamp (8) │ length (4)   │ value bytes (N) │
 *   └──────────────┴───────────────┴──────────────┴─────────────────┘
 *   Total: 20-byte header + N bytes value = wireSize bytes
 *
 *   - offset:    long, big-endian — record's absolute offset
 *   - timestamp: long, big-endian — millis since epoch
 *   - length:    int, big-endian — number of value bytes following
 *   - value:     raw bytes
 *
 * BIG-ENDIAN: ByteBuffer's default. Network byte order. Standard for binary
 *             protocols. Same as Kafka, HTTP/2, gRPC.
 *
 * HOW REAL KAFKA DOES IT:
 *       Real Kafka's RecordBatch v2 format is much richer:
 *       - Variable-length encoding (varint) for compactness
 *       - CRC-32C checksum
 *       - Headers (k/v metadata)
 *       - Compression support (gzip/snappy/lz4/zstd)
 *       - Producer ID + epoch + sequence (idempotence)
 *       We adopt that format in Phase 5.
 *
 * INTERVIEW TIP: "Why does Kafka use a custom binary format instead of JSON?"
 *       → 1. Compactness — binary is 5-10x smaller than equivalent JSON.
 *         2. Speed — no string parsing, just byte copies.
 *         3. Schema-free at the broker — Kafka stores opaque bytes.
 *         At trillions of messages per day, every byte and CPU cycle matters.
 * ============================================================================
 */
public final class RecordCodec {

    /** Size of the fixed-size header before the value bytes: 8 + 8 + 4 = 20 */
    public static final int RECORD_HEADER_SIZE = 8 + 8 + 4;

    private RecordCodec() {
        // Utility class — no instances.
    }

    // ========================================================================
    // ENCODE — LogRecord → bytes
    // ========================================================================

    /**
     * Writes a record to the given FileChannel at its current position.
     * Uses ONE channel.write() call so the OS treats it as a single I/O op
     * (more likely to be atomic for small writes — though never guaranteed
     * for crash safety, that's why we have torn-write recovery).
     *
     * After return, the channel position has advanced by wireSize bytes.
     */
    public static int writeTo(FileChannel channel, LogRecord record) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(record.wireSize());
        encodeIntoBuffer(buf, record);
        buf.flip();

        // Loop because channel.write() might be a "short write" — not all bytes
        // were accepted in one call. In practice for local files this is rare,
        // but the API allows it.
        int totalWritten = 0;
        while (buf.hasRemaining()) {
            int written = channel.write(buf);
            if (written <= 0) {
                throw new IOException("Failed to write record bytes to channel");
            }
            totalWritten += written;
        }
        return totalWritten;
    }

    /**
     * Encodes a record into an existing buffer (caller manages allocation).
     * Useful for batched writes where we want to fill a single big buffer
     * with many records.
     *
     * @return number of bytes written
     */
    public static int writeTo(ByteBuffer buf, LogRecord record) {
        return encodeIntoBuffer(buf, record);
    }

    private static int encodeIntoBuffer(ByteBuffer buf, LogRecord record) {
        int startPos = buf.position();
        buf.putLong(record.offset());        // 8 bytes
        buf.putLong(record.timestampMs());   // 8 bytes
        buf.putInt(record.value().length);   // 4 bytes
        buf.put(record.value());             // N bytes
        return buf.position() - startPos;
    }

    // ========================================================================
    // DECODE — bytes → LogRecord
    // ========================================================================

    /**
     * Reads ONE record from the channel at the given file position.
     * Uses positional reads — does NOT change the channel's shared position,
     * so it's safe to call from multiple reader threads concurrently.
     *
     * Returns the parsed LogRecord. Caller can compute the next position as
     * `position + record.wireSize()` to read the next record.
     *
     * @throws CorruptRecordException if the bytes don't form a valid record
     *         (truncated, length too large, etc.)
     */
    public static LogRecord readFrom(FileChannel channel, long position) throws IOException {
        // Step 1: Read the 20-byte fixed header
        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
        int headerRead = readFully(channel, header, position);
        if (headerRead < RECORD_HEADER_SIZE) {
            throw new CorruptRecordException(
                    "Truncated record header at position " + position
                            + ": expected " + RECORD_HEADER_SIZE + " bytes, got " + headerRead);
        }
        header.flip();

        long offset = header.getLong();
        long timestampMs = header.getLong();
        int valueLength = header.getInt();

        // Sanity check: a sane record value shouldn't be negative or insanely large.
        // 100 MB cap as a safety net — way more than we'd ever expect.
        if (valueLength < 0 || valueLength > 100 * 1024 * 1024) {
            throw new CorruptRecordException(
                    "Invalid record length at position " + position + ": " + valueLength);
        }

        // Step 2: Read the value bytes
        ByteBuffer valueBuf = ByteBuffer.allocate(valueLength);
        int valueRead = readFully(channel, valueBuf, position + RECORD_HEADER_SIZE);
        if (valueRead < valueLength) {
            throw new CorruptRecordException(
                    "Truncated record value at position " + position
                            + ": expected " + valueLength + " bytes, got " + valueRead);
        }

        return new LogRecord(offset, timestampMs, valueBuf.array());
    }

    /**
     * Reads ONE record from a buffer. Caller positions the buffer at the start
     * of the record. After return, buffer position has advanced past the record.
     *
     * Useful when we've already loaded a chunk of file bytes into a buffer
     * (faster than many small channel.read calls).
     */
    public static LogRecord readFromBuffer(ByteBuffer buf) throws CorruptRecordException {
        if (buf.remaining() < RECORD_HEADER_SIZE) {
            throw new CorruptRecordException(
                    "Buffer too short for record header: " + buf.remaining() + " bytes remaining");
        }

        long offset = buf.getLong();
        long timestampMs = buf.getLong();
        int valueLength = buf.getInt();

        if (valueLength < 0 || valueLength > 100 * 1024 * 1024) {
            throw new CorruptRecordException("Invalid record length: " + valueLength);
        }

        if (buf.remaining() < valueLength) {
            throw new CorruptRecordException(
                    "Truncated record value: expected " + valueLength
                            + " bytes, got " + buf.remaining());
        }

        byte[] value = new byte[valueLength];
        buf.get(value);

        return new LogRecord(offset, timestampMs, value);
    }

    /**
     * Helper: reads from channel into buffer, looping until full or EOF.
     * Returns the total bytes actually read.
     *
     * Why a loop? channel.read() can return fewer bytes than requested
     * (a "short read"). Our API needs ALL the bytes or fails cleanly.
     */
    private static int readFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        int totalRead = 0;
        long pos = position;
        while (buf.hasRemaining()) {
            int read = channel.read(buf, pos);
            if (read == -1) {
                break; // EOF
            }
            totalRead += read;
            pos += read;
        }
        return totalRead;
    }
}
