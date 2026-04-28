package com.ownkafka.handler;

import com.ownkafka.protocol.*;
import com.ownkafka.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ============================================================================
 * ProduceHandler — Processes PRODUCE requests (API Key 0).
 * ============================================================================
 *
 * WHAT: When a producer client wants to write a message to a topic, it sends
 *       a PRODUCE request. This handler:
 *       1. Parses the topic name and message from the request payload
 *       2. Appends the message to the log (storage)
 *       3. Returns the offset where the message was stored
 *
 * HOW REAL KAFKA PRODUCE WORKS (production scale):
 *       At companies like LinkedIn, Uber, and Netflix:
 *       1. Producer batches multiple messages for the same partition
 *       2. Optionally compresses the batch (gzip, snappy, lz4, zstd)
 *       3. Sends the batch to the partition leader broker
 *       4. Leader appends to its local commit log
 *       5. Followers pull the data and replicate it (Phase 7)
 *       6. Once all in-sync replicas (ISR) have it, it's "committed"
 *       7. Leader sends acknowledgment back to the producer
 *
 *       The acks config controls when the producer gets the acknowledgment:
 *       - acks=0: Don't wait at all (fire-and-forget, fastest, unreliable)
 *       - acks=1: Wait for leader to write (fast, might lose data if leader crashes)
 *       - acks=all: Wait for ALL replicas to write (slowest, no data loss)
 *
 * INTERVIEW TIP: "What guarantees does Kafka provide for message delivery?"
 *       → With acks=all + min.insync.replicas=2 + replication.factor=3:
 *         No data loss is guaranteed. Even if one broker fails, the message
 *         exists on at least 2 other brokers. This is the standard production
 *         config at most companies.
 *
 * PRODUCE REQUEST PAYLOAD FORMAT:
 *       [topicNameLength:2][topicName:N][messageLength:4][messageBytes:N]
 *
 * PRODUCE RESPONSE PAYLOAD FORMAT:
 *       [offset:8] — the offset where the message was stored
 * ============================================================================
 */
public class ProduceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProduceHandler.class);

    private final LogManager log;

    public ProduceHandler(LogManager log) {
        this.log = log;
    }

    /**
     * Handles a PRODUCE request.
     *
     * PHASE 2 NOTE: We now persist to disk via LogManager. The append goes
     * through CommitLog → Segment → FileChannel. After this method returns,
     * the bytes are in the OS page cache; they'll hit physical disk either
     * on the next periodic flush or on a clean shutdown.
     *
     * If the broker crashes before flush, the OS will still write whatever
     * was in the page cache during a graceful kernel shutdown. Only kill -9
     * + power loss simultaneously can lose recent writes — and even then,
     * recovery handles torn final records gracefully.
     *
     * @param request the parsed request (header + payload)
     * @return response with the assigned offset or an error
     */
    public Response handle(Request request) {
        int correlationId = request.header().correlationId();
        ByteBuffer payload = request.payload();

        try {
            // Step 1: Read the topic name from the payload
            // Format: [topicNameLength:2][topicName:N]
            String topicName = ProtocolCodec.readString(payload);

            // Step 2: Read the message bytes
            // Format: [messageLength:4][messageBytes:N]
            byte[] messageBytes = ProtocolCodec.readBytes(payload);

            // Step 3: Append (LogManager auto-creates the topic on first append)
            long offset = log.append(topicName, messageBytes);

            logger.debug("Produced message to topic='{}', offset={}, size={} bytes",
                    topicName, offset, messageBytes.length);

            // Step 4: Build success response with the assigned offset
            ByteBuffer responsePayload = ProtocolCodec.encodeProduceResponsePayload(offset);
            return new Response(correlationId, ErrorCode.NONE, responsePayload);

        } catch (IOException e) {
            // Disk I/O failed — disk full, permissions, hardware error, etc.
            logger.error("Storage I/O error during produce", e);
            return Response.error(correlationId, ErrorCode.STORAGE_ERROR);
        } catch (Exception e) {
            logger.error("Error handling produce request", e);
            return Response.error(correlationId, ErrorCode.UNKNOWN_ERROR);
        }
    }
}
