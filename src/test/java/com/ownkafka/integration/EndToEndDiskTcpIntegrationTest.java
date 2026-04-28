package com.ownkafka.integration;

import com.ownkafka.client.OwnKafkaClient;
import com.ownkafka.server.BrokerServer;
import com.ownkafka.server.RequestHandler;
import com.ownkafka.storage.LogConfig;
import com.ownkafka.storage.LogManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The headline test of Phase 2:
 *   Boot a real broker on a temp data dir → produce 100 messages over TCP
 *   → shut down → restart on the same dir → fetch all 100 → bytes match.
 *
 * If this passes, Phase 2's promise of durable storage is real.
 */
class EndToEndDiskTcpIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Produce 100 messages over TCP, restart broker, fetch all 100 — bytes match")
    void produce_restart_fetch_all() throws Exception {
        int port = findFreePort();
        LogConfig config = new LogConfig(tempDir, 4 * 1024L, 100L * 1024 * 1024, Long.MAX_VALUE, 64);

        // ---- SESSION 1: Produce 100 messages ----
        LogManager log1 = new LogManager(config);
        RequestHandler handler1 = new RequestHandler(log1);
        BrokerServer server1 = new BrokerServer(port, handler1);
        Thread t1 = new Thread(() -> { try { server1.start(); } catch (Exception ignored) {} },
                "broker-session-1");
        t1.setDaemon(true);
        t1.start();
        Thread.sleep(500);

        try (OwnKafkaClient client = new OwnKafkaClient("localhost", port)) {
            for (int i = 0; i < 100; i++) {
                long offset = client.produce("orders", "msg-" + i);
                assertEquals(i, offset);
            }
        }

        server1.shutdown();
        log1.close();
        t1.join(2000);

        // ---- SESSION 2: New broker on same data dir, fetch all 100 ----
        LogManager log2 = new LogManager(config);
        RequestHandler handler2 = new RequestHandler(log2);
        BrokerServer server2 = new BrokerServer(port, handler2);
        Thread t2 = new Thread(() -> { try { server2.start(); } catch (Exception ignored) {} },
                "broker-session-2");
        t2.setDaemon(true);
        t2.start();
        Thread.sleep(500);

        try (OwnKafkaClient client = new OwnKafkaClient("localhost", port)) {
            List<OwnKafkaClient.FetchedMessage> messages = client.fetch("orders", 0);
            assertEquals(100, messages.size(), "Expected to fetch all 100 messages after restart");

            for (int i = 0; i < 100; i++) {
                assertEquals(i, messages.get(i).offset());
                assertEquals("msg-" + i, messages.get(i).dataAsString());
            }
        }

        server2.shutdown();
        log2.close();
    }

    /** Find a free TCP port for testing. */
    private int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
