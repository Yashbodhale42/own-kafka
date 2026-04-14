package com.ownkafka.handler;

import com.ownkafka.protocol.*;
import com.ownkafka.storage.InMemoryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * ============================================================================
 * FetchHandler — Processes FETCH requests (API Key 1).
 * ============================================================================
 *
 * WHAT: When a consumer wants to read messages from a topic, it sends a
 *       FETCH request with the topic name and starting offset. This handler:
 *       1. Parses the topic name and offset from the request
 *       2. Reads messages from the log starting at that offset
 *       3. Returns the messages in the response
 *
 * HOW REAL KAFKA FETCH WORKS (production scale):
 *       1. Consumer sends FETCH to the partition leader with:
 *          - topic + partition
 *          - fetch offset (where to start reading)
 *          - max bytes (how much data to return)
 *       2. Broker finds the right segment file using the offset index
 *       3. Uses sendfile() syscall for zero-copy transfer:
 *          Data goes disk → kernel buffer → socket, BYPASSING user space
 *          This is one of Kafka's key performance tricks!
 *       4. Broker only returns messages up to the "high watermark" —
 *          messages that ALL in-sync replicas have confirmed
 *
 * ZERO-COPY (sendfile) EXPLAINED:
 *       Normal read: Disk → Kernel Buffer → User Buffer → Kernel Buffer → Socket
 *       Zero-copy:   Disk → Kernel Buffer → Socket
 *       By skipping the user-space copy, Kafka can serve data 10x faster.
 *       This is why Kafka can serve 100 MB/sec per broker easily.
 *
 * CONSUMER POLLING MODEL:
 *       Kafka consumers use a PULL model (not push):
 *       - Consumer calls poll() in a loop
 *       - Each poll() sends a FETCH request to the broker
 *       - Broker returns available messages (or empty if none)
 *       - Consumer processes messages, then polls again
 *
 *       Why pull, not push?
 *       - Consumer controls the rate (no overwhelming slow consumers)
 *       - Consumer can batch messages for efficient processing
 *       - If consumer is down, messages just accumulate (no lost pushes)
 *
 * INTERVIEW TIP: "How does Kafka achieve zero-copy?"
 *       → Using the Java FileChannel.transferTo() method, which maps to the
 *         OS sendfile() syscall. Data goes directly from disk to the network
 *         socket without being copied into application memory. This is
 *         combined with the OS page cache, so frequently-read data is
 *         served entirely from memory.
 *
 * FETCH REQUEST PAYLOAD FORMAT:
 *       [topicNameLength:2][topicName:N][offset:8]
 *
 * FETCH RESPONSE PAYLOAD FORMAT:
 *       [messageCount:4][foreach: [offset:8][messageLength:4][messageBytes:N]]
 * ============================================================================
 */
public class FetchHandler {

    private static final Logger logger = LoggerFactory.getLogger(FetchHandler.class);

    /** Maximum number of messages to return in a single FETCH response */
    private static final int MAX_FETCH_MESSAGES = 100;

    private final InMemoryLog log;

    public FetchHandler(InMemoryLog log) {
        this.log = log;
    }

    /**
     * Handles a FETCH request.
     *
     * @param request the parsed request (header + payload)
     * @return response with messages or an error
     */
    public Response handle(Request request) {
        int correlationId = request.header().correlationId();
        ByteBuffer payload = request.payload();

        try {
            // Step 1: Read the topic name
            String topicName = ProtocolCodec.readString(payload);

            // Step 2: Read the starting offset
            long offset = payload.getLong();

            // Step 3: Check if the topic exists
            if (!log.topicExists(topicName)) {
                logger.warn("Fetch from non-existent topic: {}", topicName);
                return Response.error(correlationId, ErrorCode.UNKNOWN_TOPIC);
            }

            // Step 4: Read messages from the log
            List<byte[]> messages = log.read(topicName, offset, MAX_FETCH_MESSAGES);

            logger.debug("Fetched {} messages from topic='{}', startOffset={}",
                    messages.size(), topicName, offset);

            // Step 5: Encode and return the messages
            ByteBuffer responsePayload = ProtocolCodec.encodeFetchResponsePayload(messages, offset);
            return new Response(correlationId, ErrorCode.NONE, responsePayload);

        } catch (Exception e) {
            logger.error("Error handling fetch request", e);
            return Response.error(correlationId, ErrorCode.UNKNOWN_ERROR);
        }
    }
}
