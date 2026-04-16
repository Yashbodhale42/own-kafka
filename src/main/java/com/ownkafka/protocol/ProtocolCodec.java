package com.ownkafka.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * ============================================================================
 * ProtocolCodec — Encodes and decodes our binary wire protocol.
 * ============================================================================
 *
 * WHAT: This is the "translator" between Java objects and raw bytes.
 *       When a client sends a request, the bytes arrive over TCP.
 *       This class knows how to parse those bytes into a Request object
 *       (decoding) and convert a Response object back to bytes (encoding).
 *
 * WHY BINARY ENCODING (not JSON, not Protobuf)?
 *       1. SPEED: No parsing overhead. Reading an int from bytes = 1 CPU instruction.
 *          Parsing "12345" from JSON = multiple instructions (scan digits, multiply, add).
 *       2. SIZE: The integer 1000000 takes 4 bytes in binary, 7 bytes as a string.
 *       3. KAFKA'S CHOICE: Real Kafka uses its own binary protocol for maximum speed.
 *          At millions of messages/sec, every byte and nanosecond matters.
 *
 * OUR WIRE FORMAT:
 * ┌──────────────────────────────────────────────────────────────┐
 * │ Request Frame:                                                │
 * │ [totalLength:4][apiKey:2][apiVersion:2][correlationId:4]     │
 * │ [payload bytes...]                                            │
 * │                                                               │
 * │ Response Frame:                                               │
 * │ [totalLength:4][correlationId:4][errorCode:2]                │
 * │ [payload bytes...]                                            │
 * │                                                               │
 * │ String encoding:  [length:2][UTF-8 bytes]                    │
 * │ Message encoding: [offset:8][length:4][bytes]                │
 * └──────────────────────────────────────────────────────────────┘
 *
 * The totalLength prefix is crucial — it tells the receiver exactly how
 * many bytes to read for this message. Without it, TCP's stream nature
 * makes it impossible to know where one message ends and another begins.
 * This is called "message framing" and is used in virtually all binary
 * protocols (HTTP/2, gRPC, AMQP, etc.)
 *
 * INTERVIEW TIP: "How does Kafka handle message framing over TCP?"
 *       → Length-prefixed framing. Each message starts with a 4-byte
 *         integer indicating the total size. The receiver reads 4 bytes,
 *         then reads exactly that many more bytes to get the full message.
 *         This is more efficient than delimiter-based framing (like HTTP/1.1's
 *         \r\n\r\n) because you know the exact size upfront.
 * ============================================================================
 */
public class ProtocolCodec {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolCodec.class);

    // ========================================================================
    // BYTE INSPECTOR — makes raw bytes human-readable for learning
    // ========================================================================

    /**
     * Converts a ByteBuffer's bytes into a readable hex + ASCII display.
     *
     * Example output for "produce orders Hi":
     *
     *   Offset  | Hex bytes (each byte = 2 hex digits)        | ASCII
     *   --------+---------------------------------------------+--------
     *   0000    | 00 00 00 16 00 00 00 00 00 00 00 01 00 06   | ........ ...
     *   0014    | 6F 72 64 65 72 73 00 00 00 02 48 69         | orders..Hi
     *
     * WHY HEX? Binary bytes (0-255) can't all be shown as characters.
     * Hex (base-16) is the standard way to display raw bytes — each byte
     * is shown as exactly 2 characters (00 to FF).
     * This is the same format Wireshark shows network packets.
     */
    public static String hexDump(ByteBuffer buffer) {
        // Duplicate so we don't move the original buffer's position
        ByteBuffer copy = buffer.duplicate();
        copy.rewind(); // start from the beginning

        StringBuilder sb = new StringBuilder();
        sb.append("\n  Offset  | Hex Bytes                                       | ASCII\n");
        sb.append("  --------+-------------------------------------------------+----------------\n");

        int offset = 0;
        while (copy.hasRemaining()) {
            // Start of a new row — print the offset
            sb.append(String.format("  %04X    | ", offset));

            // Collect up to 16 bytes for this row
            int rowStart = copy.position();
            int bytesInRow = Math.min(16, copy.remaining());
            byte[] row = new byte[bytesInRow];
            copy.get(row);

            // Print hex values (each byte as 2 hex digits + space)
            for (byte b : row) {
                sb.append(String.format("%02X ", b));
            }

            // Pad with spaces if row has fewer than 16 bytes
            for (int i = bytesInRow; i < 16; i++) {
                sb.append("   ");
            }

            // Print ASCII representation (. for non-printable chars)
            sb.append("| ");
            for (byte b : row) {
                char c = (b >= 32 && b < 127) ? (char) b : '.';
                sb.append(c);
            }

            sb.append("\n");
            offset += bytesInRow;
        }
        return sb.toString();
    }

    /**
     * Annotated decode — explains what each byte group means as it's parsed.
     * Call this instead of decodeRequest() when you want to see the breakdown.
     *
     * Example output:
     *   [DECODE REQUEST]
     *   Bytes 0-1  (short)  → apiKey        = 0  → PRODUCE
     *   Bytes 2-3  (short)  → apiVersion    = 0
     *   Bytes 4-7  (int)    → correlationId = 1
     *   Bytes 8-9  (short)  → topicLength   = 6
     *   Bytes 10-15 (chars) → topic         = "orders"
     *   Bytes 16-19 (int)   → msgLength     = 2
     *   Bytes 20-21 (bytes) → message       = "Hi"
     */
    public static Request decodeRequestVerbose(ByteBuffer buffer) {
        logger.debug("[DECODE REQUEST] Breaking down incoming bytes:");

        int pos = 0;

        short apiKeyCode = buffer.getShort();
        logger.debug("  Bytes {:2d}-{:2d} (short , 2 bytes) → apiKey        = {}  → {}",
                pos, pos+1, apiKeyCode, ApiKeys.fromCode(apiKeyCode));
        pos += 2;

        short apiVersion = buffer.getShort();
        logger.debug("  Bytes {:2d}-{:2d} (short , 2 bytes) → apiVersion    = {}", pos, pos+1, apiVersion);
        pos += 2;

        int correlationId = buffer.getInt();
        logger.debug("  Bytes {:2d}-{:2d} (int   , 4 bytes) → correlationId = {}", pos, pos+3, correlationId);
        pos += 4;

        // Peek at topic without consuming (for display only)
        if (buffer.remaining() >= 2) {
            buffer.mark();
            short topicLen = buffer.getShort();
            logger.debug("  Bytes {:2d}-{:2d} (short , 2 bytes) → topicLength   = {}", pos, pos+1, topicLen);
            pos += 2;

            if (buffer.remaining() >= topicLen) {
                byte[] topicBytes = new byte[topicLen];
                buffer.get(topicBytes);
                logger.debug("  Bytes {:2d}-{:2d} (chars , {} bytes) → topic         = \"{}\"",
                        pos, pos+topicLen-1, topicLen, new String(topicBytes, StandardCharsets.UTF_8));
                pos += topicLen;
            }

            if (buffer.remaining() >= 4) {
                int msgOrOffsetLen = buffer.getInt();

                if (ApiKeys.fromCode(apiKeyCode) == ApiKeys.FETCH) {
                    // For FETCH, the next 8 bytes are an offset (long)
                    logger.debug("  Bytes {:2d}-{:2d} (long  , 8 bytes) → fetchOffset   = {}", pos, pos+7, msgOrOffsetLen);
                } else {
                    // For PRODUCE, it's a message length + data
                    logger.debug("  Bytes {:2d}-{:2d} (int   , 4 bytes) → msgLength     = {}", pos, pos+3, msgOrOffsetLen);
                    pos += 4;
                    if (buffer.remaining() >= msgOrOffsetLen) {
                        byte[] msgBytes = new byte[msgOrOffsetLen];
                        buffer.get(msgBytes);
                        logger.debug("  Bytes {:2d}-{:2d} (bytes , {} bytes) → message       = \"{}\"",
                                pos, pos+msgOrOffsetLen-1, msgOrOffsetLen,
                                new String(msgBytes, StandardCharsets.UTF_8));
                    }
                }
            }

            // Reset so decodeRequest() can read normally
            buffer.reset();
            // Re-position to before topic
            buffer.position(buffer.position() - 2);
        }

        // Now rewind fully and do actual decode
        buffer.rewind();
        return decodeRequest(buffer);
    }

    // ========================================================================
    // REQUEST ENCODING (Client → Broker)
    // ========================================================================

    /**
     * Encodes a PRODUCE request into bytes ready to send over TCP.
     *
     * Wire format:
     * [totalLength:4][apiKey:2][apiVersion:2][correlationId:4]
     * [topicNameLength:2][topicName:N][messageLength:4][messageBytes:N]
     *
     * @param correlationId  unique ID for this request
     * @param topicName      which topic to produce to (e.g., "orders")
     * @param message        the message bytes to store
     * @return ByteBuffer containing the fully framed request
     */
    public static ByteBuffer encodeProduceRequest(int correlationId, String topicName, byte[] message) {
        // Convert topic name to bytes for wire transmission
        byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);

        // Calculate total payload size:
        // topicNameLength(2) + topicName(N) + messageLength(4) + message(N)
        int payloadSize = 2 + topicBytes.length + 4 + message.length;

        // Total frame size = header(8) + payload
        int totalSize = RequestHeader.HEADER_SIZE + payloadSize;

        // Allocate buffer: 4 bytes for length prefix + the message itself
        ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);

        // Write the length prefix (how many bytes follow)
        buffer.putInt(totalSize);

        // Write the request header
        buffer.putShort(ApiKeys.PRODUCE.code());   // apiKey = 0 (PRODUCE)
        buffer.putShort((short) 0);                 // apiVersion = 0
        buffer.putInt(correlationId);               // correlationId

        // Write the payload: topic name (length-prefixed string)
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        // Write the payload: message (length-prefixed bytes)
        buffer.putInt(message.length);
        buffer.put(message);

        // Flip the buffer: switches from "write mode" to "read mode"
        // After writing, position is at the end. flip() sets position=0
        // and limit=previous position, so readers start from the beginning.
        buffer.flip();

        // ---- DEBUG LOGGING ----
        logger.debug("[ENCODE PRODUCE REQUEST]");
        logger.debug("  Building frame for: topic='{}', message='{}', correlationId={}",
                topicName, new String(message, StandardCharsets.UTF_8), correlationId);
        logger.debug("  Size breakdown: lengthPrefix(4) + apiKey(2) + apiVersion(2) + correlationId(4)" +
                " + topicLen(2) + topic({}) + msgLen(4) + msg({}) = {} bytes total",
                topicBytes.length, message.length, 4 + totalSize);
        logger.debug("  Hex dump of full frame: {}", hexDump(buffer));
        // -----------------------

        return buffer;
    }

    /**
     * Encodes a FETCH request into bytes ready to send over TCP.
     *
     * Wire format:
     * [totalLength:4][apiKey:2][apiVersion:2][correlationId:4]
     * [topicNameLength:2][topicName:N][offset:8]
     *
     * @param correlationId  unique ID for this request
     * @param topicName      which topic to fetch from
     * @param offset         the starting offset (position) to read from
     * @return ByteBuffer containing the fully framed request
     */
    public static ByteBuffer encodeFetchRequest(int correlationId, String topicName, long offset) {
        byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);

        // Payload: topicNameLength(2) + topicName(N) + offset(8)
        int payloadSize = 2 + topicBytes.length + 8;
        int totalSize = RequestHeader.HEADER_SIZE + payloadSize;

        ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);
        buffer.putInt(totalSize);

        // Header
        buffer.putShort(ApiKeys.FETCH.code());
        buffer.putShort((short) 0);
        buffer.putInt(correlationId);

        // Payload
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putLong(offset);

        buffer.flip();

        // ---- DEBUG LOGGING ----
        logger.debug("[ENCODE FETCH REQUEST]");
        logger.debug("  Fetching from: topic='{}', startOffset={}, correlationId={}",
                topicName, offset, correlationId);
        logger.debug("  Size breakdown: lengthPrefix(4) + apiKey(2) + apiVersion(2) + correlationId(4)" +
                " + topicLen(2) + topic({}) + offset(8) = {} bytes total",
                topicBytes.length, 4 + totalSize);
        logger.debug("  Hex dump of full frame: {}", hexDump(buffer));
        // -----------------------

        return buffer;
    }

    // ========================================================================
    // REQUEST DECODING (Broker receives bytes → parses into Request object)
    // ========================================================================

    /**
     * Decodes a raw byte buffer into a Request object.
     *
     * IMPORTANT: The caller must have already read the 4-byte length prefix
     *            and given us exactly that many bytes. This method parses
     *            the header and wraps the remaining bytes as payload.
     *
     * @param buffer  raw bytes of the request (WITHOUT the length prefix)
     * @return parsed Request object
     */
    public static Request decodeRequest(ByteBuffer buffer) {
        // ---- DEBUG LOGGING ----
        logger.debug("[DECODE REQUEST] Raw bytes received from client: {}", hexDump(buffer));
        // -----------------------

        // Read the header fields in order (order matters in binary protocols!)
        short apiKeyCode = buffer.getShort();     // bytes 0-1
        short apiVersion = buffer.getShort();     // bytes 2-3
        int correlationId = buffer.getInt();      // bytes 4-7

        // Convert numeric API key to our enum
        ApiKeys apiKey = ApiKeys.fromCode(apiKeyCode);

        // The rest of the buffer is the payload
        // slice() creates a new ByteBuffer sharing the same underlying data
        // but with its own position and limit — so the payload starts from
        // the current position.
        ByteBuffer payload = buffer.slice();

        // ---- DEBUG LOGGING ----
        logger.debug("[DECODE REQUEST] Parsed header →  apiKey={} ({}), apiVersion={}, correlationId={}",
                apiKeyCode, apiKey, apiVersion, correlationId);
        logger.debug("[DECODE REQUEST] Payload starts at byte 8, {} bytes remaining for handler to parse",
                payload.remaining());
        // -----------------------

        return new Request(
                new RequestHeader(apiKey, apiVersion, correlationId),
                payload
        );
    }

    // ========================================================================
    // RESPONSE ENCODING (Broker → Client)
    // ========================================================================

    /**
     * Encodes a Response into bytes ready to send over TCP.
     *
     * Wire format:
     * [totalLength:4][correlationId:4][errorCode:2][payload...]
     */
    public static ByteBuffer encodeResponse(Response response) {
        int payloadSize = response.payload() != null ? response.payload().remaining() : 0;

        // Total frame: correlationId(4) + errorCode(2) + payload
        int totalSize = 4 + 2 + payloadSize;

        ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);
        buffer.putInt(totalSize);                        // Length prefix
        buffer.putInt(response.correlationId());         // Correlation ID
        buffer.putShort(response.errorCode().code());    // Error code

        // Write payload if present
        if (payloadSize > 0) {
            buffer.put(response.payload());
        }

        buffer.flip();

        // ---- DEBUG LOGGING ----
        logger.debug("[ENCODE RESPONSE]");
        logger.debug("  correlationId={}, errorCode={} ({}), payloadBytes={}",
                response.correlationId(),
                response.errorCode().code(), response.errorCode(),
                payloadSize);
        logger.debug("  Size breakdown: lengthPrefix(4) + correlationId(4) + errorCode(2) + payload({}) = {} bytes",
                payloadSize, 4 + totalSize);
        logger.debug("  Hex dump of full frame: {}", hexDump(buffer));
        // -----------------------

        return buffer;
    }

    // ========================================================================
    // RESPONSE DECODING (Client receives bytes → parses into Response)
    // ========================================================================

    /**
     * Decodes a raw byte buffer into a Response object.
     *
     * @param buffer  raw bytes of the response (WITHOUT the length prefix)
     * @return parsed Response object
     */
    public static Response decodeResponse(ByteBuffer buffer) {
        // ---- DEBUG LOGGING ----
        logger.debug("[DECODE RESPONSE] Raw bytes received from broker: {}", hexDump(buffer));
        // -----------------------

        int correlationId    = buffer.getInt();    // bytes 0-3
        short errorCodeValue = buffer.getShort();  // bytes 4-5
        ErrorCode errorCode  = ErrorCode.fromCode(errorCodeValue);

        // Remaining bytes are the payload
        ByteBuffer payload = buffer.slice();

        // ---- DEBUG LOGGING ----
        logger.debug("[DECODE RESPONSE] Parsed → correlationId={}, errorCode={} ({}), payloadBytes={}",
                correlationId, errorCodeValue, errorCode, payload.remaining());
        if (errorCode == ErrorCode.NONE && payload.remaining() == 8) {
            // Peek at the offset (PRODUCE response)
            logger.debug("[DECODE RESPONSE] This looks like a PRODUCE response → offset={}",
                    payload.duplicate().getLong());
        } else if (errorCode == ErrorCode.NONE && payload.remaining() >= 4) {
            // Peek at message count (FETCH response)
            logger.debug("[DECODE RESPONSE] This looks like a FETCH response → messageCount={}",
                    payload.duplicate().getInt());
        }
        // -----------------------

        return new Response(correlationId, errorCode, payload);
    }

    // ========================================================================
    // PAYLOAD HELPERS — for reading/writing topic names and messages
    // ========================================================================

    /**
     * Reads a length-prefixed UTF-8 string from the buffer.
     * Format: [length:2][UTF-8 bytes]
     *
     * This is how strings are encoded in binary protocols.
     * You can't just write raw characters because the receiver wouldn't
     * know where the string ends. Length-prefixing solves this.
     */
    public static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads a length-prefixed byte array from the buffer.
     * Format: [length:4][bytes]
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Encodes a PRODUCE response payload.
     * Contains just the offset where the message was stored.
     *
     * Format: [offset:8]
     */
    public static ByteBuffer encodeProduceResponsePayload(long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(offset);
        buffer.flip();
        return buffer;
    }

    /**
     * Encodes a FETCH response payload.
     * Contains a list of messages, each with its offset.
     *
     * Format: [messageCount:4][foreach: [offset:8][messageLength:4][messageBytes:N]]
     *
     * @param messages  list of messages to include in the response
     * @param startOffset  the offset of the first message
     */
    public static ByteBuffer encodeFetchResponsePayload(List<byte[]> messages, long startOffset) {
        // Calculate total size needed
        int totalSize = 4; // messageCount
        for (byte[] msg : messages) {
            totalSize += 8 + 4 + msg.length; // offset + length + data per message
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(messages.size());

        long offset = startOffset;
        for (byte[] msg : messages) {
            buffer.putLong(offset++);       // message offset
            buffer.putInt(msg.length);       // message length
            buffer.put(msg);                 // message data
        }

        buffer.flip();
        return buffer;
    }
}
