package com.ownkafka.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ProtocolCodec — verifying that encoding and decoding are
 * perfectly symmetrical (encode then decode returns the original data).
 *
 * This is called "roundtrip testing" and is essential for any protocol
 * implementation. If encode and decode don't match perfectly, clients
 * and servers will misunderstand each other.
 */
class ProtocolCodecTest {

    @Test
    @DisplayName("Produce request: encode → decode roundtrip preserves data")
    void testProduceRequestRoundtrip() {
        // Encode a produce request
        ByteBuffer encoded = ProtocolCodec.encodeProduceRequest(42, "orders", "Hello Kafka!".getBytes());

        // Skip the 4-byte length prefix (the server reads this separately)
        encoded.getInt(); // skip length

        // Decode the request
        Request request = ProtocolCodec.decodeRequest(encoded);

        // Verify header fields
        assertEquals(ApiKeys.PRODUCE, request.header().apiKey());
        assertEquals(0, request.header().apiVersion());
        assertEquals(42, request.header().correlationId());

        // Verify payload: read topic name and message
        String topic = ProtocolCodec.readString(request.payload());
        byte[] message = ProtocolCodec.readBytes(request.payload());

        assertEquals("orders", topic);
        assertEquals("Hello Kafka!", new String(message));
    }

    @Test
    @DisplayName("Fetch request: encode → decode roundtrip preserves data")
    void testFetchRequestRoundtrip() {
        ByteBuffer encoded = ProtocolCodec.encodeFetchRequest(99, "events", 42L);

        encoded.getInt(); // skip length prefix

        Request request = ProtocolCodec.decodeRequest(encoded);

        assertEquals(ApiKeys.FETCH, request.header().apiKey());
        assertEquals(99, request.header().correlationId());

        String topic = ProtocolCodec.readString(request.payload());
        long offset = request.payload().getLong();

        assertEquals("events", topic);
        assertEquals(42L, offset);
    }

    @Test
    @DisplayName("Response: encode → decode roundtrip preserves data")
    void testResponseRoundtrip() {
        // Create a response with a payload
        ByteBuffer payload = ByteBuffer.allocate(8);
        payload.putLong(100L); // offset
        payload.flip();

        Response original = new Response(7, ErrorCode.NONE, payload);

        // Encode the response
        ByteBuffer encoded = ProtocolCodec.encodeResponse(original);

        // Skip the length prefix
        encoded.getInt();

        // Decode the response
        Response decoded = ProtocolCodec.decodeResponse(encoded);

        assertEquals(7, decoded.correlationId());
        assertEquals(ErrorCode.NONE, decoded.errorCode());
        assertEquals(100L, decoded.payload().getLong());
    }

    @Test
    @DisplayName("Error response with no payload encodes/decodes correctly")
    void testErrorResponseRoundtrip() {
        Response errorResponse = Response.error(55, ErrorCode.UNKNOWN_TOPIC);

        ByteBuffer encoded = ProtocolCodec.encodeResponse(errorResponse);
        encoded.getInt(); // skip length

        Response decoded = ProtocolCodec.decodeResponse(encoded);

        assertEquals(55, decoded.correlationId());
        assertEquals(ErrorCode.UNKNOWN_TOPIC, decoded.errorCode());
        assertEquals(0, decoded.payload().remaining()); // empty payload
    }

    @Test
    @DisplayName("Fetch response payload encodes multiple messages correctly")
    void testFetchResponsePayload() {
        List<byte[]> messages = List.of(
                "first message".getBytes(),
                "second message".getBytes(),
                "third message".getBytes()
        );

        ByteBuffer payload = ProtocolCodec.encodeFetchResponsePayload(messages, 5L);

        // Read back
        int count = payload.getInt();
        assertEquals(3, count);

        // Message 1
        assertEquals(5L, payload.getLong());        // offset
        byte[] data1 = ProtocolCodec.readBytes(payload);
        assertEquals("first message", new String(data1));

        // Message 2
        assertEquals(6L, payload.getLong());        // offset
        byte[] data2 = ProtocolCodec.readBytes(payload);
        assertEquals("second message", new String(data2));

        // Message 3
        assertEquals(7L, payload.getLong());        // offset
        byte[] data3 = ProtocolCodec.readBytes(payload);
        assertEquals("third message", new String(data3));
    }

    @Test
    @DisplayName("ApiKeys.fromCode resolves known codes correctly")
    void testApiKeysFromCode() {
        assertEquals(ApiKeys.PRODUCE, ApiKeys.fromCode((short) 0));
        assertEquals(ApiKeys.FETCH, ApiKeys.fromCode((short) 1));
    }

    @Test
    @DisplayName("ApiKeys.fromCode throws for unknown codes")
    void testApiKeysFromCodeUnknown() {
        assertThrows(IllegalArgumentException.class, () -> ApiKeys.fromCode((short) 999));
    }

    @Test
    @DisplayName("Unicode topic names are handled correctly")
    void testUnicodeTopicName() {
        String unicodeTopic = "events-日本語";
        ByteBuffer encoded = ProtocolCodec.encodeProduceRequest(1, unicodeTopic, "test".getBytes());

        encoded.getInt(); // skip length
        Request request = ProtocolCodec.decodeRequest(encoded);

        String decoded = ProtocolCodec.readString(request.payload());
        assertEquals(unicodeTopic, decoded);
    }
}
