package com.ownkafka.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for InMemoryLog — verifying message storage, retrieval, and thread safety.
 */
class InMemoryLogTest {

    private InMemoryLog log;

    @BeforeEach
    void setUp() {
        log = new InMemoryLog();
    }

    @Test
    @DisplayName("Append returns sequential offsets starting from 0")
    void testAppendReturnsSequentialOffsets() {
        assertEquals(0, log.append("topic1", "msg0".getBytes()));
        assertEquals(1, log.append("topic1", "msg1".getBytes()));
        assertEquals(2, log.append("topic1", "msg2".getBytes()));
    }

    @Test
    @DisplayName("Different topics have independent offset sequences")
    void testIndependentTopicOffsets() {
        assertEquals(0, log.append("topicA", "a0".getBytes()));
        assertEquals(0, log.append("topicB", "b0".getBytes()));
        assertEquals(1, log.append("topicA", "a1".getBytes()));
        assertEquals(1, log.append("topicB", "b1".getBytes()));
    }

    @Test
    @DisplayName("Read returns messages from the specified offset")
    void testReadFromOffset() {
        log.append("topic1", "msg0".getBytes());
        log.append("topic1", "msg1".getBytes());
        log.append("topic1", "msg2".getBytes());

        // Read from offset 1 — should get msg1 and msg2
        List<byte[]> messages = log.read("topic1", 1, 10);
        assertEquals(2, messages.size());
        assertEquals("msg1", new String(messages.get(0)));
        assertEquals("msg2", new String(messages.get(1)));
    }

    @Test
    @DisplayName("Read from offset 0 returns all messages")
    void testReadFromBeginning() {
        log.append("topic1", "first".getBytes());
        log.append("topic1", "second".getBytes());

        List<byte[]> messages = log.read("topic1", 0, 10);
        assertEquals(2, messages.size());
        assertEquals("first", new String(messages.get(0)));
        assertEquals("second", new String(messages.get(1)));
    }

    @Test
    @DisplayName("Read respects maxMessages limit")
    void testReadMaxMessages() {
        for (int i = 0; i < 10; i++) {
            log.append("topic1", ("msg" + i).getBytes());
        }

        List<byte[]> messages = log.read("topic1", 0, 3);
        assertEquals(3, messages.size());
    }

    @Test
    @DisplayName("Read from non-existent topic returns empty list")
    void testReadNonExistentTopic() {
        List<byte[]> messages = log.read("no-such-topic", 0, 10);
        assertTrue(messages.isEmpty());
    }

    @Test
    @DisplayName("Read from offset beyond end returns empty list")
    void testReadBeyondEnd() {
        log.append("topic1", "msg0".getBytes());

        List<byte[]> messages = log.read("topic1", 99, 10);
        assertTrue(messages.isEmpty());
    }

    @Test
    @DisplayName("getEndOffset returns the next offset to be assigned")
    void testGetEndOffset() {
        assertEquals(0, log.getEndOffset("topic1"));

        log.append("topic1", "msg0".getBytes());
        assertEquals(1, log.getEndOffset("topic1"));

        log.append("topic1", "msg1".getBytes());
        assertEquals(2, log.getEndOffset("topic1"));
    }

    @Test
    @DisplayName("topicExists returns correct values")
    void testTopicExists() {
        assertFalse(log.topicExists("topic1"));

        log.append("topic1", "msg".getBytes());
        assertTrue(log.topicExists("topic1"));
        assertFalse(log.topicExists("topic2"));
    }

    @Test
    @DisplayName("Concurrent appends don't lose messages")
    void testConcurrentAppends() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // Each thread appends 100 messages to the same topic
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                for (int m = 0; m < messagesPerThread; m++) {
                    log.append("concurrent-topic", ("msg-" + m).getBytes());
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        // All messages should be there
        int expectedTotal = threadCount * messagesPerThread;
        assertEquals(expectedTotal, log.getEndOffset("concurrent-topic"));
    }
}
