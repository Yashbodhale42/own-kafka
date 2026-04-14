package com.ownkafka.server;

import com.ownkafka.protocol.ProtocolCodec;
import com.ownkafka.protocol.Request;
import com.ownkafka.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ============================================================================
 * BrokerServer — The TCP server that accepts client connections.
 * ============================================================================
 *
 * WHAT: This is the network layer of our Kafka broker. It:
 *       1. Listens on a TCP port (default: 9092, same as real Kafka)
 *       2. Accepts new client connections
 *       3. Reads incoming bytes from clients
 *       4. Delegates parsing and handling to RequestHandler
 *       5. Sends responses back to clients
 *
 * ARCHITECTURE: Thread-per-connection with a thread pool.
 *       - One acceptor thread: listens for new connections (the main thread)
 *       - A thread pool handles each client connection
 *
 *       This is simpler than NIO Selector-based approaches and works well
 *       for hundreds of connections. For thousands, you'd use NIO/Netty.
 *
 * HOW REAL KAFKA'S NETWORKING WORKS:
 *       Real Kafka has a more sophisticated architecture:
 *       1. Acceptor Thread: Only accepts new connections (like a receptionist)
 *       2. N Processor Threads: Handle read/write I/O using NIO (like waiters)
 *       3. M Handler Threads (RequestHandlerPool): Process business logic (like chefs)
 *       4. A RequestChannel connects processors <-> handlers via queues
 *
 *       This separation allows Kafka to handle 100k+ connections efficiently.
 *       In Phase 5, we'll switch to Netty which handles NIO for us.
 *
 * WHY THREAD-PER-CONNECTION FOR NOW?
 *       1. SIMPLER to understand — no NIO selectors, no event loop complexity
 *       2. PORTABLE — works on all platforms (NIO Selector has Windows quirks)
 *       3. GOOD ENOUGH — for our learning purpose, handling 10-100 clients
 *       4. EDUCATIONAL — this is how servers worked before NIO (Tomcat, etc.)
 *
 *       The tradeoff: each connection uses ~1MB stack memory per thread.
 *       1000 connections = 1GB just for thread stacks. NIO avoids this
 *       by using one thread for many connections.
 *
 * INTERVIEW TIP: "How does Kafka handle many concurrent connections?"
 *       → Real Kafka uses NIO with a reactor pattern. One acceptor thread,
 *         N processor threads for network I/O, and M handler threads for
 *         request processing. This decouples network I/O from business logic.
 *         In Phase 5, we'll switch to Netty (which abstracts NIO).
 *
 * INTERVIEW TIP: "What is the reactor pattern?"
 *       → A design pattern for handling I/O. A single thread (reactor) monitors
 *         multiple I/O sources using a selector/epoll. When an event occurs
 *         (data ready, connection accepted), it dispatches to the right handler.
 *         Used by Kafka, Redis, Nginx, Node.js, Netty.
 *
 * INTERVIEW TIP: "Thread-per-connection vs NIO vs virtual threads?"
 *       → Thread-per-connection: simple but doesn't scale past ~10K connections
 *         NIO/Selector: scalable but complex (Kafka, Netty use this)
 *         Virtual threads (Java 21+): simple AND scalable — the future!
 *         Kafka is exploring virtual threads for future versions.
 * ============================================================================
 */
public class BrokerServer {

    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    /** The port to listen on. Real Kafka default is also 9092. */
    private final int port;

    /** Handles the business logic for each request */
    private final RequestHandler requestHandler;

    /**
     * Thread pool for handling client connections.
     *
     * We use a cached thread pool — it creates threads as needed and reuses
     * idle threads. Good for variable workloads. In production Kafka, the
     * thread pool sizes are carefully tuned:
     * - num.network.threads (default 3): processor threads for I/O
     * - num.io.threads (default 8): handler threads for business logic
     */
    private final ExecutorService clientThreadPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true); // Daemon threads don't prevent JVM shutdown
        t.setName("client-handler-" + t.threadId());
        return t;
    });

    /** The server socket that listens for connections */
    private ServerSocket serverSocket;

    /** Controls the accept loop — set to false to shut down gracefully */
    private volatile boolean running = false;

    /** Tracks connected clients for monitoring */
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    public BrokerServer(int port, RequestHandler requestHandler) {
        this.port = port;
        this.requestHandler = requestHandler;
    }

    /**
     * Starts the broker server.
     * This method blocks — it runs the accept loop until shutdown is called.
     *
     * The startup sequence mirrors real Kafka:
     * 1. Open a server socket and bind to the port
     * 2. Enter the accept loop
     * 3. For each new connection, spawn a handler thread
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        logger.info("OwnKafka broker started on port {}", port);
        logger.info("Waiting for client connections...");

        // ====================================================================
        // THE ACCEPT LOOP
        //
        // This loop runs continuously, accepting new client connections.
        // accept() blocks until a client connects.
        // Each connection is handled by a thread from the pool.
        //
        // In real Kafka, this runs in the Acceptor thread, and connections
        // are distributed to Processor threads round-robin.
        // ====================================================================
        while (running) {
            try {
                // Block until a new client connects
                Socket clientSocket = serverSocket.accept();

                int connCount = activeConnections.incrementAndGet();
                logger.info("New client connected: {} (active connections: {})",
                        clientSocket.getRemoteSocketAddress(), connCount);

                // Hand off to a thread from the pool
                clientThreadPool.submit(() -> handleClient(clientSocket));

            } catch (SocketException e) {
                // Expected when serverSocket is closed during shutdown
                if (running) {
                    logger.error("Error accepting connection", e);
                }
            }
        }

        logger.info("Broker accept loop ended");
    }

    /**
     * Handles a single client connection.
     *
     * This method runs in its own thread (from the thread pool).
     * It reads requests in a loop, processes each one, and sends back responses.
     *
     * The loop continues until the client disconnects or an error occurs.
     *
     * MESSAGE FRAMING:
     * TCP is a stream protocol — it doesn't have message boundaries.
     * We use length-prefixed framing:
     *   1. Read 4 bytes → this is the message length
     *   2. Read exactly that many bytes → this is the message
     *   3. Repeat
     *
     * DataInputStream.readInt() and readFully() handle the byte-level details.
     */
    private void handleClient(Socket clientSocket) {
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        try (clientSocket;
             DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {

            // Read requests in a loop until client disconnects
            while (running && !clientSocket.isClosed()) {
                try {
                    // Step 1: Read the 4-byte length prefix
                    // readInt() reads exactly 4 bytes and converts to int (big-endian)
                    // This tells us how many bytes the actual message is
                    int messageLength = in.readInt();

                    // Step 2: Read the message body (exactly messageLength bytes)
                    // readFully() blocks until ALL bytes are read — handles
                    // partial TCP reads automatically
                    byte[] messageBytes = new byte[messageLength];
                    in.readFully(messageBytes);

                    logger.debug("Read message: {} bytes from {}", messageLength, clientAddress);

                    // Step 3: Parse the raw bytes into a Request object
                    ByteBuffer messageBuffer = ByteBuffer.wrap(messageBytes);
                    Request request = ProtocolCodec.decodeRequest(messageBuffer);

                    // Step 4: Handle the request (business logic)
                    Response response = requestHandler.handleRequest(request);

                    // Step 5: Encode the response to bytes
                    ByteBuffer responseBuffer = ProtocolCodec.encodeResponse(response);

                    // Step 6: Write the response back to the client
                    // The response already includes the 4-byte length prefix
                    // (added by encodeResponse), so we just write all bytes.
                    byte[] responseBytes = new byte[responseBuffer.remaining()];
                    responseBuffer.get(responseBytes);
                    out.write(responseBytes);
                    out.flush();

                    logger.debug("Sent response: {} bytes to {}", responseBytes.length, clientAddress);

                } catch (EOFException e) {
                    // Client closed the connection gracefully
                    logger.info("Client disconnected (EOF): {}", clientAddress);
                    break;
                }
            }

        } catch (SocketException e) {
            // Connection reset — client disconnected abruptly
            logger.info("Client disconnected: {} ({})", clientAddress, e.getMessage());
        } catch (IOException e) {
            logger.error("Error handling client {}", clientAddress, e);
        } finally {
            int connCount = activeConnections.decrementAndGet();
            logger.debug("Active connections: {}", connCount);
        }
    }

    /**
     * Gracefully shuts down the broker.
     * Called from a shutdown hook or test code.
     *
     * In real Kafka, shutdown involves:
     * 1. Stop accepting new connections
     * 2. Complete in-flight requests
     * 3. Transfer partition leadership to other brokers
     * 4. Flush data to disk
     * 5. Close all connections
     *
     * We just close the server socket and thread pool.
     */
    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close(); // This interrupts the accept() call
            }
        } catch (IOException e) {
            logger.debug("Error closing server socket", e);
        }
        clientThreadPool.shutdownNow();
        logger.info("Shutdown signal received");
    }
}
