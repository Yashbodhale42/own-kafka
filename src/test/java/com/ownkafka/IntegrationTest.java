package com.ownkafka;

import com.ownkafka.client.OwnKafkaClient;
import com.ownkafka.server.BrokerServer;
import com.ownkafka.server.RequestHandler;
import com.ownkafka.storage.LogConfig;
import com.ownkafka.storage.LogManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Quick integration test — starts the broker in a thread, connects a client,
 * produces and fetches messages. Run manually to verify end-to-end behavior.
 *
 * Phase 2: now uses disk-backed LogManager in a temp dir.
 *
 * Usage: java -cp target/classes;target/test-classes com.ownkafka.IntegrationTest
 */
public class IntegrationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== OwnKafka Integration Test ===\n");

        // Start broker on a test port with a temp data dir
        int port = 19092;
        Path tempDir = Files.createTempDirectory("ownkafka-integration-test-");
        System.out.println("Temp data dir: " + tempDir);

        LogManager log = new LogManager(LogConfig.forTesting(tempDir));
        RequestHandler handler = new RequestHandler(log);
        BrokerServer server = new BrokerServer(port, handler);

        Thread serverThread = new Thread(() -> {
            try { server.start(); } catch (Exception e) { e.printStackTrace(); }
        }, "broker-thread");
        serverThread.setDaemon(true);
        serverThread.start();

        // Give server time to start
        Thread.sleep(1000);

        try (OwnKafkaClient client = new OwnKafkaClient("localhost", port)) {
            // Test 1: Produce messages
            System.out.println("--- Test 1: Producing messages ---");
            long offset0 = client.produce("orders", "Order #1001 - Pizza");
            System.out.println("Produced at offset: " + offset0);

            long offset1 = client.produce("orders", "Order #1002 - Burger");
            System.out.println("Produced at offset: " + offset1);

            long offset2 = client.produce("orders", "Order #1003 - Sushi");
            System.out.println("Produced at offset: " + offset2);

            assert offset0 == 0 : "First offset should be 0";
            assert offset1 == 1 : "Second offset should be 1";
            assert offset2 == 2 : "Third offset should be 2";
            System.out.println("PASSED: Offsets are sequential\n");

            // Test 2: Fetch all messages from beginning
            System.out.println("--- Test 2: Fetching from beginning ---");
            List<OwnKafkaClient.FetchedMessage> allMessages = client.fetch("orders", 0);
            for (OwnKafkaClient.FetchedMessage msg : allMessages) {
                System.out.println("  " + msg);
            }
            assert allMessages.size() == 3 : "Should have 3 messages";
            System.out.println("PASSED: Got all 3 messages\n");

            // Test 3: Fetch from offset 2
            System.out.println("--- Test 3: Fetching from offset 2 ---");
            List<OwnKafkaClient.FetchedMessage> fromOffset2 = client.fetch("orders", 2);
            assert fromOffset2.size() == 1 : "Should have 1 message";
            assert fromOffset2.get(0).dataAsString().contains("Sushi") : "Should be the Sushi order";
            System.out.println("  " + fromOffset2.get(0));
            System.out.println("PASSED: Correct message at offset 2\n");

            // Test 4: Multiple topics
            System.out.println("--- Test 4: Multiple topics ---");
            client.produce("logs", "Server started");
            client.produce("logs", "Request received");
            client.produce("orders", "Order #1004 - Ramen");

            List<OwnKafkaClient.FetchedMessage> logMessages = client.fetch("logs", 0);
            List<OwnKafkaClient.FetchedMessage> orderMessages = client.fetch("orders", 0);
            assert logMessages.size() == 2 : "Logs topic should have 2 messages";
            assert orderMessages.size() == 4 : "Orders topic should have 4 messages";
            System.out.println("  Logs: " + logMessages.size() + " messages");
            System.out.println("  Orders: " + orderMessages.size() + " messages");
            System.out.println("PASSED: Topics are independent\n");

            System.out.println("=== ALL INTEGRATION TESTS PASSED ===");
        }

        server.shutdown();
        log.close();
    }
}
