package com.ownkafka.server;

import com.ownkafka.handler.FetchHandler;
import com.ownkafka.handler.ProduceHandler;
import com.ownkafka.protocol.ErrorCode;
import com.ownkafka.protocol.ProtocolCodec;
import com.ownkafka.protocol.Request;
import com.ownkafka.protocol.Response;
import com.ownkafka.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ============================================================================
 * RequestHandler — Routes incoming requests to the correct handler.
 * ============================================================================
 *
 * WHAT: After the server reads and parses a request, it needs to figure out
 *       what to DO with it. The RequestHandler acts as a router:
 *       - API key 0 (PRODUCE) → ProduceHandler
 *       - API key 1 (FETCH)   → FetchHandler
 *       - Unknown API key      → error response
 *
 * WHY A SEPARATE ROUTER?
 *       Separation of concerns! The TCP server handles networking (bytes in,
 *       bytes out). The RequestHandler handles business logic (what do we do
 *       with this request?). This makes it easy to:
 *       - Add new API types without touching the server
 *       - Unit test handlers without setting up a TCP connection
 *       - Swap out the network layer (NIO → Netty in Phase 5)
 *
 * HOW REAL KAFKA DOES IT:
 *       Real Kafka has a KafkaApis class that contains a giant match statement:
 *       case PRODUCE => handleProduceRequest()
 *       case FETCH => handleFetchRequest()
 *       case METADATA => handleMetadataRequest()
 *       ... (60+ cases!)
 *       Each handler is a method that reads the request, does the work, and
 *       creates a response. Same pattern as what we're building here.
 *
 * INTERVIEW TIP: "Walk me through what happens when a Kafka producer sends a message."
 *       1. Producer serializes the record (key, value, headers)
 *       2. Partitioner decides which partition to send to (hash(key) % partitions)
 *       3. Record is batched with others going to the same partition
 *       4. Sender thread sends the batch as a PRODUCE request over TCP
 *       5. Broker receives bytes, parses the request header
 *       6. RequestHandler routes to ProduceHandler based on API key
 *       7. ProduceHandler appends to the commit log, assigns offset
 *       8. Response is sent back with the offset (or error code)
 *       9. Producer callback is triggered (success or failure)
 * ============================================================================
 */
public class RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final ProduceHandler produceHandler;
    private final FetchHandler fetchHandler;

    public RequestHandler(LogManager log) {
        this.produceHandler = new ProduceHandler(log);
        this.fetchHandler = new FetchHandler(log);
    }

    /**
     * Processes a request and returns a response.
     * This is the main routing method — called for every request the broker receives.
     *
     * @param request the parsed request from the client
     * @return the response to send back
     */
    public Response handleRequest(Request request) {
        logger.debug("Handling request: apiKey={}, correlationId={}",
                request.header().apiKey(), request.header().correlationId());

        // Route to the appropriate handler based on the API key
        return switch (request.header().apiKey()) {
            case PRODUCE -> produceHandler.handle(request);
            case FETCH -> fetchHandler.handle(request);

            // If we get here, the client sent an API key we don't support yet.
            // In real Kafka, this returns UNSUPPORTED_VERSION error.
            // We'll add more cases as we implement more APIs (Phase 3+).
        };
    }
}
