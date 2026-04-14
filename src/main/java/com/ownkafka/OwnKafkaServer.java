package com.ownkafka;

import com.ownkafka.server.BrokerServer;
import com.ownkafka.server.RequestHandler;
import com.ownkafka.storage.InMemoryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ============================================================================
 * OwnKafkaServer — The main entry point for our Kafka clone broker.
 * ============================================================================
 *
 * WHAT: This is the "main" class that starts everything up. It:
 *       1. Creates the storage layer (InMemoryLog)
 *       2. Creates the request handler (wired to the storage)
 *       3. Creates and starts the TCP server
 *       4. Registers a shutdown hook for graceful cleanup
 *
 * HOW TO RUN:
 *       ./mvnw.cmd exec:java -Dexec.mainClass=com.ownkafka.OwnKafkaServer
 *       or: java -cp target/classes com.ownkafka.OwnKafkaServer
 *
 * HOW REAL KAFKA STARTS:
 *       Real Kafka's startup (KafkaServer.scala) is much more complex:
 *       1. Load configuration from server.properties
 *       2. Initialize metrics and monitoring
 *       3. Start the log manager (manage segment files on disk)
 *       4. Start the socket server (accept connections)
 *       5. Start the replica manager (handle replication)
 *       6. Start the group coordinator (manage consumer groups)
 *       7. Start the controller (manage partition leaders)
 *       8. Register with ZooKeeper/KRaft
 *       9. Start the request handler pool
 *
 *       We'll add many of these in later phases. For now, we just need
 *       the essentials: storage + handler + server.
 *
 * GRACEFUL SHUTDOWN:
 *       When you press Ctrl+C, the JVM sends a shutdown signal. Our shutdown
 *       hook catches this and cleanly stops the server. In real Kafka, the
 *       shutdown process is critical:
 *       - Flush uncommitted data to disk
 *       - Transfer partition leadership to other brokers
 *       - Notify other brokers that this one is leaving
 *       - Close all client connections
 *       A clean shutdown takes about 5-10 seconds in production Kafka.
 *
 * INTERVIEW TIP: "How do you ensure zero data loss during Kafka broker shutdown?"
 *       → Controlled shutdown: The broker transfers leadership of all its
 *         partitions to other in-sync replicas BEFORE shutting down. This
 *         ensures no downtime for producers/consumers. If a broker crashes
 *         (not clean shutdown), the controller detects it via heartbeat
 *         timeout and triggers leader election for affected partitions.
 * ============================================================================
 */
public class OwnKafkaServer {

    private static final Logger logger = LoggerFactory.getLogger(OwnKafkaServer.class);

    /** Default port — same as real Kafka. Producers/consumers connect here. */
    private static final int DEFAULT_PORT = 9092;

    public static void main(String[] args) {
        // Allow port override via command line argument
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                logger.warn("Invalid port '{}', using default {}", args[0], DEFAULT_PORT);
            }
        }

        // ================================================================
        // WIRING PHASE — Connect all the components together
        //
        // This is a simple form of "dependency injection" — each component
        // receives its dependencies through the constructor.
        //
        // In real Kafka, this wiring is much more complex (dozens of
        // components with intricate dependencies). Spring Boot and
        // similar frameworks automate this, but Kafka does it manually
        // for maximum control.
        // ================================================================

        // 1. Storage layer — where messages are stored
        InMemoryLog log = new InMemoryLog();

        // 2. Request handler — business logic (produce, fetch)
        RequestHandler requestHandler = new RequestHandler(log);

        // 3. TCP server — network layer
        BrokerServer server = new BrokerServer(port, requestHandler);

        // ================================================================
        // SHUTDOWN HOOK — Graceful shutdown on Ctrl+C
        //
        // The JVM calls shutdown hooks when the process is terminating.
        // We use this to cleanly stop the server (close connections,
        // flush buffers, etc.)
        //
        // Runtime.getRuntime().addShutdownHook() registers a thread
        // that runs during JVM shutdown. It's a best practice for any
        // server application.
        // ================================================================
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered — stopping broker...");
            server.shutdown();
        }, "shutdown-hook"));

        // ================================================================
        // START THE SERVER
        //
        // This call BLOCKS — it runs the NIO event loop.
        // The server will keep running until shutdown() is called.
        // ================================================================
        try {
            logger.info("============================================");
            logger.info("     OWN KAFKA BROKER — Phase 1");
            logger.info("============================================");
            logger.info("Starting broker on port {}...", port);
            server.start();
        } catch (Exception e) {
            logger.error("Failed to start broker", e);
            System.exit(1);
        }
    }
}
