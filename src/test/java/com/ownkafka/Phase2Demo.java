package com.ownkafka;

import com.ownkafka.client.OwnKafkaClient;
import com.ownkafka.server.BrokerServer;
import com.ownkafka.server.RequestHandler;
import com.ownkafka.storage.LogConfig;
import com.ownkafka.storage.LogManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Phase 2 demo — proves that messages survive a full broker restart.
 *
 * Round 1: Start broker, produce 5 messages, shut down cleanly.
 * Round 2: Start broker on the SAME data dir, fetch — all 5 must be there.
 *
 * Run: java -cp "target/classes;target/test-classes;$(cat target/cp.txt)" com.ownkafka.Phase2Demo
 */
public class Phase2Demo {

    public static void main(String[] args) throws Exception {
        int port = 19099;
        Path dataDir = Path.of("phase2-demo-data");

        // Clean up from previous runs
        if (Files.exists(dataDir)) {
            try (Stream<Path> stream = Files.walk(dataDir)) {
                stream.sorted((a, b) -> b.toString().compareTo(a.toString()))
                        .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) {} });
            }
        }

        System.out.println("================================================");
        System.out.println("  PHASE 2 DEMO — Restart-survives demonstration");
        System.out.println("================================================");
        System.out.println("Data dir: " + dataDir.toAbsolutePath());

        // ============================================================
        // ROUND 1 — Start broker, produce 5 messages, shut down
        // ============================================================
        System.out.println("\n--- ROUND 1: Producing messages ---");
        {
            LogManager log = new LogManager(new LogConfig(
                    dataDir, 1024L * 1024, 100L * 1024 * 1024,
                    Long.MAX_VALUE, 4096));
            BrokerServer server = new BrokerServer(port, new RequestHandler(log));

            Thread serverThread = new Thread(() -> {
                try { server.start(); } catch (Exception ignored) {}
            }, "broker-r1");
            serverThread.setDaemon(true);
            serverThread.start();
            Thread.sleep(500);

            try (OwnKafkaClient client = new OwnKafkaClient("localhost", port)) {
                System.out.println("  produce → offset " + client.produce("orders", "Pizza"));
                System.out.println("  produce → offset " + client.produce("orders", "Burger"));
                System.out.println("  produce → offset " + client.produce("orders", "Sushi"));
                System.out.println("  produce → offset " + client.produce("orders", "Ramen"));
                System.out.println("  produce → offset " + client.produce("orders", "Curry"));
            }

            server.shutdown();
            log.close();
            System.out.println("  Broker R1 shut down cleanly. Data persisted to disk.");
        }

        // Show files on disk between runs
        System.out.println("\n--- Files on disk after Round 1 ---");
        try (Stream<Path> stream = Files.walk(dataDir)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                try { System.out.println("  " + p + " (" + Files.size(p) + " bytes)"); }
                catch (Exception ignored) {}
            });
        }

        // ============================================================
        // ROUND 2 — Brand new broker process simulation, same data dir
        // ============================================================
        System.out.println("\n--- ROUND 2: Restarting broker, fetching messages ---");
        {
            LogManager log = new LogManager(new LogConfig(
                    dataDir, 1024L * 1024, 100L * 1024 * 1024,
                    Long.MAX_VALUE, 4096));
            BrokerServer server = new BrokerServer(port, new RequestHandler(log));

            Thread serverThread = new Thread(() -> {
                try { server.start(); } catch (Exception ignored) {}
            }, "broker-r2");
            serverThread.setDaemon(true);
            serverThread.start();
            Thread.sleep(500);

            try (OwnKafkaClient client = new OwnKafkaClient("localhost", port)) {
                List<OwnKafkaClient.FetchedMessage> recovered = client.fetch("orders", 0L);
                System.out.println("  Recovered " + recovered.size() + " messages:");
                for (OwnKafkaClient.FetchedMessage msg : recovered) {
                    System.out.println("    " + msg);
                }

                if (recovered.size() == 5) {
                    System.out.println("\n  PHASE 2 GOAL ACHIEVED: All 5 messages survived restart!");
                } else {
                    System.out.println("\n  FAILED — expected 5 messages, got " + recovered.size());
                }
            }

            server.shutdown();
            log.close();
        }

        System.out.println("\n================================================");
        System.out.println("  Demo complete.");
        System.out.println("================================================");
    }
}
