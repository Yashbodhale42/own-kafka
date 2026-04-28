package com.ownkafka.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LogManager — top-level façade managing all topics.
 */
class LogManagerTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Multiple topics are independent")
    void independentTopics() throws IOException {
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            mgr.append("topicA", "a0".getBytes());
            mgr.append("topicB", "b0".getBytes());
            mgr.append("topicA", "a1".getBytes());

            assertEquals(2L, mgr.getEndOffset("topicA"));
            assertEquals(1L, mgr.getEndOffset("topicB"));
        }
    }

    @Test
    @DisplayName("topicExists is true after append")
    void topicExistsAfterAppend() throws IOException {
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            assertFalse(mgr.topicExists("orders"));
            mgr.append("orders", "msg".getBytes());
            assertTrue(mgr.topicExists("orders"));
        }
    }

    @Test
    @DisplayName("Topics survive restart")
    void topicsRecoverAfterRestart() throws IOException {
        // First session: create some data
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            mgr.append("orders", "first".getBytes());
            mgr.append("orders", "second".getBytes());
            mgr.append("logs", "system started".getBytes());
        }

        // Second session: should see the same data
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            assertTrue(mgr.topicExists("orders"));
            assertTrue(mgr.topicExists("logs"));
            assertEquals(2L, mgr.getEndOffset("orders"));
            assertEquals(1L, mgr.getEndOffset("logs"));

            List<byte[]> orders = mgr.read("orders", 0L, 100);
            assertEquals(2, orders.size());
            assertEquals("first", new String(orders.get(0)));
            assertEquals("second", new String(orders.get(1)));
        }
    }

    @Test
    @DisplayName("Read from non-existent topic returns empty list")
    void readUnknownTopic_empty() throws IOException {
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            assertTrue(mgr.read("ghost", 0L, 10).isEmpty());
        }
    }

    @Test
    @DisplayName("Concurrent producers to same topic produce monotonic gapless offsets")
    void concurrentProducers_monotonicOffsets() throws Exception {
        int threadCount = 4;
        int messagesPerThread = 100;
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Set<Long> seenOffsets = ConcurrentSafeSet();

        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                exec.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            long off = mgr.append("concurrent", ("t" + threadId + "-m" + i).getBytes());
                            synchronized (seenOffsets) {
                                seenOffsets.add(off);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            exec.shutdown();

            // All offsets must be distinct
            int total = threadCount * messagesPerThread;
            assertEquals(total, seenOffsets.size(), "Some offsets were duplicated");

            // Offsets must be 0..(total-1) — gapless
            AtomicLong sum = new AtomicLong();
            seenOffsets.forEach(sum::addAndGet);
            long expectedSum = (long) total * (total - 1) / 2;
            assertEquals(expectedSum, sum.get(), "Offsets are not gapless from 0..N-1");
        }
    }

    private static <T> Set<T> ConcurrentSafeSet() {
        return java.util.Collections.synchronizedSet(new HashSet<>());
    }

    @Test
    @DisplayName("createTopic explicitly creates an empty topic")
    void createTopicExplicit() throws IOException {
        try (LogManager mgr = new LogManager(LogConfig.forTesting(tempDir))) {
            mgr.createTopic("explicit");
            assertTrue(mgr.topicExists("explicit"));
            assertEquals(0L, mgr.getEndOffset("explicit"));
        }
    }
}
