package com.ownkafka.protocol;

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
        // Read the header fields in order (order matters in binary protocols!)
        short apiKeyCode = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();

        // Convert numeric API key to our enum
        ApiKeys apiKey = ApiKeys.fromCode(apiKeyCode);

        // The rest of the buffer is the payload
        // slice() creates a new ByteBuffer sharing the same underlying data
        // but with its own position and limit — so the payload starts from
        // the current position.
        ByteBuffer payload = buffer.slice();

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
        int correlationId = buffer.getInt();
        short errorCodeValue = buffer.getShort();
        ErrorCode errorCode = ErrorCode.fromCode(errorCodeValue);

        // Remaining bytes are the payload
        ByteBuffer payload = buffer.slice();

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
