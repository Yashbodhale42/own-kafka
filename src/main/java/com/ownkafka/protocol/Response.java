package com.ownkafka.protocol;

import java.nio.ByteBuffer;

/**
 * ============================================================================
 * Response — A response from the broker back to the client.
 * ============================================================================
 *
 * WHAT: After the broker processes a request, it sends back a response
 *       containing: the correlationId (so the client knows which request
 *       this is answering), an error code, and the response payload.
 *
 * WHY ALWAYS INCLUDE ERROR CODE?
 *       Even successful responses include an error code (NONE = 0).
 *       This makes parsing uniform — the client always reads the error
 *       code first. If it's non-zero, skip parsing the payload and
 *       handle the error. Simple and predictable.
 *
 *       In real Kafka, error codes are per-partition within a response.
 *       So a Produce to 3 partitions might succeed for 2 and fail for 1.
 *       We simplify to one error code per response for now.
 *
 * RESPONSE PAYLOAD varies by API:
 *   - PRODUCE response: the offset where the message was stored [offset:8]
 *   - FETCH response: [messageCount:4] followed by messages
 * ============================================================================
 */
public record Response(
        /** Echoed from the request — lets client match response to request */
        int correlationId,

        /** Error code — NONE(0) means success */
        ErrorCode errorCode,

        /**
         * Response-specific payload.
         * Can be null/empty for error responses (the error code says it all).
         */
        ByteBuffer payload
) {
    /**
     * Convenience factory for error responses with no payload.
     * Used when something goes wrong and there's no data to return.
     */
    public static Response error(int correlationId, ErrorCode errorCode) {
        return new Response(correlationId, errorCode, ByteBuffer.allocate(0));
    }
}
