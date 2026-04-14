package com.ownkafka.server;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

/**
 * ============================================================================
 * ClientSession — Manages the state of a single client TCP connection.
 * ============================================================================
 *
 * WHAT: When a client connects to the broker over TCP, we create a ClientSession
 *       to track that connection's state: the bytes received so far, the bytes
 *       waiting to be sent, etc.
 *
 * WHY DO WE NEED THIS?
 *       TCP is a STREAM protocol — bytes arrive in chunks, not as discrete messages.
 *       A single read() from the socket might give you:
 *       - Half a message (the rest arrives in the next read)
 *       - One complete message
 *       - Two and a half messages
 *
 *       ClientSession handles "message framing" — accumulating bytes until we
 *       have a complete message (determined by the 4-byte length prefix).
 *
 *       Think of it like reading a book over someone's shoulder on a bus.
 *       They might turn the page mid-sentence. You need to remember what
 *       you read on the previous page to understand the full sentence.
 *
 * HOW REAL KAFKA HANDLES THIS:
 *       Real Kafka uses Netty (a high-performance networking framework) which
 *       handles framing automatically. We're using raw NIO to understand the
 *       fundamentals first. In Phase 5, we'll switch to Netty.
 *
 * INTERVIEW TIP: "What is TCP framing and why is it needed?"
 *       → TCP is a byte stream, not a message protocol. It doesn't know where
 *         one message ends and another begins. Framing adds boundaries.
 *         Common approaches:
 *         1. Length-prefixed (Kafka, gRPC): [4-byte length][data] — efficient
 *         2. Delimiter-based (HTTP/1.1): data ends at \r\n\r\n — simple but slow
 *         3. Fixed-size: every message is exactly N bytes — wasteful
 *         Kafka uses length-prefixed because it's the fastest to parse.
 * ============================================================================
 */
public class ClientSession {

    /**
     * Buffer for accumulating incoming bytes.
     * We start with 1MB — more than enough for most requests.
     * In real Kafka, this is configurable (socket.request.max.bytes, default 100MB).
     */
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024); // 1 MB

    /**
     * Queue of complete responses waiting to be sent to this client.
     * We use a Queue because responses must be sent in order (FIFO).
     *
     * LinkedList implements Queue — good for FIFO operations with frequent add/remove.
     */
    private final Queue<ByteBuffer> writeQueue = new LinkedList<>();

    /**
     * Returns the read buffer for the NIO channel to write incoming bytes into.
     * The buffer is in "write mode" — NIO will put bytes starting at position.
     */
    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    /**
     * Tries to extract a complete message from the read buffer.
     *
     * Protocol: [4-byte length][message bytes]
     *
     * Returns null if we don't have a complete message yet (need more bytes).
     * Returns a ByteBuffer containing JUST the message (without the length prefix).
     *
     * This method may be called multiple times after a single read() —
     * because one read might contain multiple complete messages.
     *
     * HOW IT WORKS:
     * 1. Flip the buffer to read mode
     * 2. Check if we have at least 4 bytes (the length prefix)
     * 3. Read the length value (peek, don't consume yet)
     * 4. Check if we have enough bytes for the full message
     * 5. If yes: extract the message, compact the buffer for the next read
     * 6. If no: reset position and wait for more data
     */
    public ByteBuffer extractMessage() {
        // Flip: switch from write mode to read mode.
        // Before flip: position=where we stopped writing, limit=capacity
        // After flip:  position=0, limit=where we stopped writing
        readBuffer.flip();

        // Need at least 4 bytes for the length prefix
        if (readBuffer.remaining() < 4) {
            // Not enough data yet — switch back to write mode
            readBuffer.compact();
            return null;
        }

        // Peek at the length (mark current position so we can go back)
        readBuffer.mark();
        int messageLength = readBuffer.getInt();

        // Check if we have the full message
        if (readBuffer.remaining() < messageLength) {
            // Not enough data for the full message — put the 4 bytes back
            readBuffer.reset();
            readBuffer.compact(); // Preserves unread data, switches to write mode
            return null;
        }

        // We have a complete message! Extract it.
        byte[] messageBytes = new byte[messageLength];
        readBuffer.get(messageBytes);

        // Compact: move any remaining bytes (next message) to the beginning
        // of the buffer, and switch to write mode for the next read
        readBuffer.compact();

        return ByteBuffer.wrap(messageBytes);
    }

    /**
     * Queues a response to be sent to this client.
     * The response ByteBuffer should be in read mode (ready to read from).
     */
    public void enqueueResponse(ByteBuffer response) {
        writeQueue.add(response);
    }

    /**
     * Returns the next response to send, or null if the queue is empty.
     * Used by the server's write handler to send pending responses.
     */
    public ByteBuffer peekWriteBuffer() {
        return writeQueue.peek();
    }

    /**
     * Removes the front response from the queue (called after it's fully sent).
     */
    public void completeWrite() {
        writeQueue.poll();
    }

    /**
     * Returns true if there are responses waiting to be sent.
     * The server uses this to decide whether to register for OP_WRITE events.
     */
    public boolean hasDataToWrite() {
        return !writeQueue.isEmpty();
    }
}
