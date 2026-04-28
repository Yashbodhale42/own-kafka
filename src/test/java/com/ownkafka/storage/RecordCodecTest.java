package com.ownkafka.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RecordCodec — verifies the on-disk binary record format.
 */
class RecordCodecTest {

    @Test
    @DisplayName("Encode then decode returns the same record")
    void roundTrip_singleRecord() throws Exception {
        LogRecord original = new LogRecord(42L, 1234567890L, "Hello".getBytes());

        ByteBuffer buf = ByteBuffer.allocate(original.wireSize());
        RecordCodec.writeTo(buf, original);
        buf.flip();

        LogRecord decoded = RecordCodec.readFromBuffer(buf);

        assertEquals(original.offset(), decoded.offset());
        assertEquals(original.timestampMs(), decoded.timestampMs());
        assertArrayEquals(original.value(), decoded.value());
    }

    @Test
    @DisplayName("Empty value bytes encode/decode correctly")
    void roundTrip_emptyValue() throws Exception {
        LogRecord original = new LogRecord(0L, 0L, new byte[0]);

        ByteBuffer buf = ByteBuffer.allocate(original.wireSize());
        RecordCodec.writeTo(buf, original);
        buf.flip();

        LogRecord decoded = RecordCodec.readFromBuffer(buf);
        assertEquals(0, decoded.value().length);
    }

    @Test
    @DisplayName("Large value (1 MB) encodes/decodes correctly")
    void roundTrip_largeValue() throws Exception {
        byte[] big = new byte[1024 * 1024];
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i % 256);
        LogRecord original = new LogRecord(7L, 999L, big);

        ByteBuffer buf = ByteBuffer.allocate(original.wireSize());
        RecordCodec.writeTo(buf, original);
        buf.flip();

        LogRecord decoded = RecordCodec.readFromBuffer(buf);
        assertArrayEquals(big, decoded.value());
    }

    @Test
    @DisplayName("Truncated header throws CorruptRecordException")
    void truncatedHeader_throws() {
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.putLong(1L); // only 8 bytes — header needs 20
        buf.flip();

        assertThrows(CorruptRecordException.class, () -> RecordCodec.readFromBuffer(buf));
    }

    @Test
    @DisplayName("Truncated value bytes throws CorruptRecordException")
    void truncatedValue_throws() {
        ByteBuffer buf = ByteBuffer.allocate(20 + 5); // claim length 100 but provide 5
        buf.putLong(1L);
        buf.putLong(0L);
        buf.putInt(100);
        buf.put(new byte[5]);
        buf.flip();

        assertThrows(CorruptRecordException.class, () -> RecordCodec.readFromBuffer(buf));
    }

    @Test
    @DisplayName("Negative length throws CorruptRecordException")
    void negativeLength_throws() {
        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.putLong(0L);
        buf.putLong(0L);
        buf.putInt(-1); // bogus length
        buf.flip();

        assertThrows(CorruptRecordException.class, () -> RecordCodec.readFromBuffer(buf));
    }

    @Test
    @DisplayName("wireSize matches actual bytes written")
    void wireSize_matchesActualBytes() {
        LogRecord r = new LogRecord(0L, 0L, "test message".getBytes());
        ByteBuffer buf = ByteBuffer.allocate(r.wireSize());
        int written = RecordCodec.writeTo(buf, r);
        assertEquals(r.wireSize(), written);
    }

    @Test
    @DisplayName("RECORD_HEADER_SIZE constant equals 20")
    void headerSizeConstant() {
        assertEquals(20, RecordCodec.RECORD_HEADER_SIZE);
    }
}
