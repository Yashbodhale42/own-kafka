package com.ownkafka.client;

import com.ownkafka.protocol.ErrorCode;
import com.ownkafka.protocol.ProtocolCodec;
import com.ownkafka.protocol.Response;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ============================================================================
 * OwnKafkaClient — A simple TCP client for our Kafka clone.
 * ============================================================================
 *
 * WHAT: This is the "producer" and "consumer" client library. It:
 *       1. Connects to the broker over TCP
 *       2. Sends produce/fetch requests using our binary protocol
 *       3. Reads and parses responses
 *
 * HOW REAL KAFKA CLIENTS WORK:
 *       Real Kafka clients (KafkaProducer, KafkaConsumer in Java) are much
 *       more sophisticated:
 *
 *       KafkaProducer:
 *       - Maintains a buffer of pending messages (RecordAccumulator)
 *       - Batches messages by partition for efficiency
 *       - A background Sender thread flushes batches to brokers
 *       - Supports compression (gzip, snappy, lz4, zstd)
 *       - Retries failed sends with configurable backoff
 *       - Calls user-provided callbacks on success/failure
 *
 *       KafkaConsumer:
 *       - Maintains consumer group membership (heartbeats)
 *       - Auto-commits offsets on a configurable interval
 *       - Handles partition rebalancing (when consumers join/leave the group)
 *       - Fetches in batches with configurable max.poll.records
 *
 *       Our client is a simple blocking client — good enough for testing.
 *       In Phase 5, we could make it smarter (batching, async, retries).
 *
 * INTERVIEW TIP: "How does the Kafka producer achieve high throughput?"
 *       → Batching + compression + async sending.
 *         Messages are accumulated in memory (batched by partition).
 *         A background thread sends full batches to brokers.
 *         Compression (lz4 is fastest) shrinks the batch before sending.
 *         The producer doesn't wait for each message — it fires-and-forgets
 *         (with acks=1) or batches acknowledgments (with acks=all).
 *         This can achieve millions of messages/sec from a single producer.
 * ============================================================================
 */
public class OwnKafkaClient implements AutoCloseable {

    private final SocketChannel channel;

    /**
     * Correlation ID counter — atomically increments for each request.
     * Ensures every request gets a unique ID, even from multiple threads.
     *
     * In real Kafka clients, this is how pipelined requests are matched
     * to responses. The client might send requests 1, 2, 3 without waiting,
     * and match responses as they come back using the correlation IDs.
     */
    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);

    /**
     * Connects to the broker at the given host and port.
     *
     * @param host broker hostname (e.g., "localhost")
     * @param port broker port (e.g., 9092)
     * @throws IOException if connection fails
     */
    public OwnKafkaClient(String host, int port) throws IOException {
        // Open a blocking TCP connection to the broker
        // Blocking mode is simpler for a client — we send a request and
        // wait for the response. The server uses non-blocking NIO because
        // it handles many connections; the client only has one.
        this.channel = SocketChannel.open();
        this.channel.connect(new InetSocketAddress(host, port));
    }

    /**
     * Sends a message to the specified topic and returns the assigned offset.
     *
     * This is the equivalent of KafkaProducer.send() in the real client.
     *
     * @param topic   the topic to produce to
     * @param message the message string (converted to UTF-8 bytes)
     * @return the offset where the message was stored
     * @throws IOException if the send fails
     */
    public long produce(String topic, String message) throws IOException {
        int correlationId = correlationIdCounter.incrementAndGet();

        // Encode the produce request into binary format
        ByteBuffer request = ProtocolCodec.encodeProduceRequest(
                correlationId, topic, message.getBytes());

        // Send the request to the broker
        sendFully(request);

        // Read and parse the response
        Response response = readResponse();

        // Check for errors
        if (response.errorCode() != ErrorCode.NONE) {
            throw new IOException("Produce failed with error: " + response.errorCode());
        }

        // Extract the offset from the response payload
        return response.payload().getLong();
    }

    /**
     * Fetches messages from the specified topic starting at the given offset.
     *
     * This is the equivalent of KafkaConsumer.poll() in the real client.
     *
     * @param topic  the topic to fetch from
     * @param offset the starting offset
     * @return list of FetchedMessage objects containing offset and data
     * @throws IOException if the fetch fails
     */
    public List<FetchedMessage> fetch(String topic, long offset) throws IOException {
        int correlationId = correlationIdCounter.incrementAndGet();

        // Encode the fetch request
        ByteBuffer request = ProtocolCodec.encodeFetchRequest(correlationId, topic, offset);

        // Send and receive
        sendFully(request);
        Response response = readResponse();

        if (response.errorCode() != ErrorCode.NONE) {
            throw new IOException("Fetch failed with error: " + response.errorCode());
        }

        // Parse the response payload into individual messages
        return parseFetchResponse(response.payload());
    }

    /**
     * Writes the entire buffer to the channel.
     *
     * Why a loop? Because channel.write() might not write all bytes at once.
     * The OS socket buffer might be full, or the network might be slow.
     * We keep calling write() until all bytes are sent.
     *
     * In real Kafka clients, sends are buffered and batched for efficiency.
     */
    private void sendFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Reads a complete response from the channel.
     *
     * Protocol: [4-byte length][response data]
     * 1. Read 4 bytes to get the length
     * 2. Read exactly that many bytes for the response body
     * 3. Parse into a Response object
     */
    private Response readResponse() throws IOException {
        // Step 1: Read the 4-byte length prefix
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        readFully(lengthBuffer);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        // Step 2: Read the response body
        ByteBuffer responseBuffer = ByteBuffer.allocate(length);
        readFully(responseBuffer);
        responseBuffer.flip();

        // Step 3: Parse into a Response object
        return ProtocolCodec.decodeResponse(responseBuffer);
    }

    /**
     * Reads bytes from the channel until the buffer is full.
     *
     * Similar to sendFully — channel.read() might not fill the buffer
     * in one call. We loop until we have all the bytes we need.
     */
    private void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                throw new IOException("Connection closed by broker");
            }
        }
    }

    /**
     * Parses the FETCH response payload into a list of messages.
     *
     * Format: [messageCount:4][foreach: [offset:8][messageLength:4][messageBytes:N]]
     */
    private List<FetchedMessage> parseFetchResponse(ByteBuffer payload) {
        int messageCount = payload.getInt();
        List<FetchedMessage> messages = new ArrayList<>(messageCount);

        for (int i = 0; i < messageCount; i++) {
            long msgOffset = payload.getLong();
            byte[] data = ProtocolCodec.readBytes(payload);
            messages.add(new FetchedMessage(msgOffset, data));
        }

        return messages;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Represents a message retrieved from a FETCH response.
     *
     * Contains the message's offset (position in the log) and its data.
     * In real Kafka, this is called ConsumerRecord and also includes:
     * key, value, headers, timestamp, partition, topic, serialized sizes.
     */
    public record FetchedMessage(long offset, byte[] data) {
        /** Returns the message data as a UTF-8 string (convenience method) */
        public String dataAsString() {
            return new String(data);
        }

        @Override
        public String toString() {
            return String.format("offset=%d, data='%s'", offset, dataAsString());
        }
    }
}
