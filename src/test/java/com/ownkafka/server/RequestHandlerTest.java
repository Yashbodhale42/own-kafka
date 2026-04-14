package com.ownkafka.server;

import com.ownkafka.protocol.*;
import com.ownkafka.storage.InMemoryLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RequestHandler — verifying produce and fetch logic end-to-end.
 *
 * These tests bypass the TCP layer and directly test the request handling
 * logic. This is a form of "integration testing" for our business logic
 * without needing network setup.
 */
class RequestHandlerTest {

    private RequestHandler handler;
    private InMemoryLog log;

    @BeforeEach
    void setUp() {
        log = new InMemoryLog();
        handler = new RequestHandler(log);
    }

    @Test
    @DisplayName("Produce request stores message and returns offset")
    void testProduceStoresMessage() {
        // Build a produce request directly (as if received from the network)
        Request request = createProduceRequest(1, "test-topic", "Hello World");

        Response response = handler.handleRequest(request);

        // Verify success response
        assertEquals(1, response.correlationId());
        assertEquals(ErrorCode.NONE, response.errorCode());

        // Verify the offset is 0 (first message)
        long offset = response.payload().getLong();
        assertEquals(0, offset);

        // Verify the message is actually in the log
        var messages = log.read("test-topic", 0, 10);
        assertEquals(1, messages.size());
        assertEquals("Hello World", new String(messages.get(0)));
    }

    @Test
    @DisplayName("Multiple produce requests return incrementing offsets")
    void testProduceIncrementsOffset() {
        Response r1 = handler.handleRequest(createProduceRequest(1, "topic1", "msg0"));
        Response r2 = handler.handleRequest(createProduceRequest(2, "topic1", "msg1"));
        Response r3 = handler.handleRequest(createProduceRequest(3, "topic1", "msg2"));

        assertEquals(0, r1.payload().getLong());
        assertEquals(1, r2.payload().getLong());
        assertEquals(2, r3.payload().getLong());
    }

    @Test
    @DisplayName("Fetch returns messages starting from the given offset")
    void testFetchReturnsMessages() {
        // Produce some messages first
        handler.handleRequest(createProduceRequest(1, "topic1", "first"));
        handler.handleRequest(createProduceRequest(2, "topic1", "second"));
        handler.handleRequest(createProduceRequest(3, "topic1", "third"));

        // Fetch from offset 1
        Request fetchRequest = createFetchRequest(10, "topic1", 1);
        Response response = handler.handleRequest(fetchRequest);

        assertEquals(ErrorCode.NONE, response.errorCode());

        // Parse the fetch response payload
        ByteBuffer payload = response.payload();
        int messageCount = payload.getInt();
        assertEquals(2, messageCount); // "second" and "third"

        // First message: offset=1, data="second"
        assertEquals(1L, payload.getLong());
        byte[] data1 = ProtocolCodec.readBytes(payload);
        assertEquals("second", new String(data1));

        // Second message: offset=2, data="third"
        assertEquals(2L, payload.getLong());
        byte[] data2 = ProtocolCodec.readBytes(payload);
        assertEquals("third", new String(data2));
    }

    @Test
    @DisplayName("Fetch from non-existent topic returns UNKNOWN_TOPIC error")
    void testFetchNonExistentTopic() {
        Request fetchRequest = createFetchRequest(1, "no-such-topic", 0);
        Response response = handler.handleRequest(fetchRequest);

        assertEquals(ErrorCode.UNKNOWN_TOPIC, response.errorCode());
    }

    @Test
    @DisplayName("Produce auto-creates topic if it doesn't exist")
    void testProduceAutoCreatesTopic() {
        assertFalse(log.topicExists("new-topic"));

        handler.handleRequest(createProduceRequest(1, "new-topic", "first msg"));

        assertTrue(log.topicExists("new-topic"));
    }

    @Test
    @DisplayName("Fetch from offset beyond end returns 0 messages")
    void testFetchBeyondEnd() {
        handler.handleRequest(createProduceRequest(1, "topic1", "only msg"));

        Request fetchRequest = createFetchRequest(2, "topic1", 100);
        Response response = handler.handleRequest(fetchRequest);

        assertEquals(ErrorCode.NONE, response.errorCode());
        int messageCount = response.payload().getInt();
        assertEquals(0, messageCount);
    }

    // ========================================================================
    // Helper methods to build Request objects for testing
    // ========================================================================

    private Request createProduceRequest(int correlationId, String topic, String message) {
        byte[] topicBytes = topic.getBytes();
        byte[] msgBytes = message.getBytes();

        // Build payload: [topicLen:2][topic:N][msgLen:4][msg:N]
        ByteBuffer payload = ByteBuffer.allocate(2 + topicBytes.length + 4 + msgBytes.length);
        payload.putShort((short) topicBytes.length);
        payload.put(topicBytes);
        payload.putInt(msgBytes.length);
        payload.put(msgBytes);
        payload.flip();

        return new Request(
                new RequestHeader(ApiKeys.PRODUCE, (short) 0, correlationId),
                payload
        );
    }

    private Request createFetchRequest(int correlationId, String topic, long offset) {
        byte[] topicBytes = topic.getBytes();

        // Build payload: [topicLen:2][topic:N][offset:8]
        ByteBuffer payload = ByteBuffer.allocate(2 + topicBytes.length + 8);
        payload.putShort((short) topicBytes.length);
        payload.put(topicBytes);
        payload.putLong(offset);
        payload.flip();

        return new Request(
                new RequestHeader(ApiKeys.FETCH, (short) 0, correlationId),
                payload
        );
    }
}
