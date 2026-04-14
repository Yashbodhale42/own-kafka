package com.ownkafka.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ============================================================================
 * InMemoryLog — A simple in-memory message store (temporary, replaced in Phase 2).
 * ============================================================================
 *
 * WHAT: Stores messages in memory, organized by topic. Each message gets an
 *       offset (sequential number starting from 0). Producers append messages,
 *       consumers read from a given offset.
 *
 * WHY IN-MEMORY FIRST?
 *       We want to get the TCP server working quickly without worrying about
 *       disk I/O. In Phase 2, we'll replace this with a real commit log
 *       (append-only files on disk). This is a common development pattern:
 *       start simple, swap out components later.
 *
 * HOW REAL KAFKA STORES MESSAGES:
 *       Kafka stores messages in a "commit log" — an append-only file on disk.
 *       Each partition has its own log directory with segment files:
 *         data/orders-0/00000000000000000000.log  (first segment)
 *         data/orders-0/00000000000000050000.log  (second segment, starts at offset 50000)
 *       Messages are written sequentially (like writing in a notebook from top to bottom).
 *       This sequential I/O is why Kafka can write 700 MB/sec to disk — almost as fast
 *       as memory! (Because disk sequential reads/writes avoid seek time.)
 *
 * THREAD SAFETY:
 *       Multiple clients may produce and consume simultaneously. We need thread-safe
 *       data structures. CopyOnWriteArrayList is used for the message list because:
 *       - Writes are relatively infrequent (batched in real Kafka)
 *       - Reads are very frequent (many consumers)
 *       - It provides a consistent snapshot for reads without locking
 *       In Phase 2 with disk storage, we'll use different synchronization.
 *
 * INTERVIEW TIP: "What is a commit log and why does Kafka use it?"
 *       → A commit log is an append-only, ordered, persistent data structure.
 *         Kafka uses it because:
 *         1. Appending is O(1) — always writes to the end, never modifies old data
 *         2. Sequential I/O is fastest on both HDD and SSD
 *         3. Immutable data simplifies replication (just copy the bytes)
 *         4. Consumers can independently read at their own pace (via offsets)
 *
 * INTERVIEW TIP: "What is an offset in Kafka?"
 *       → An offset is a sequential, monotonically increasing number assigned to
 *         each message within a partition. It's like a line number. Consumers track
 *         their offset to know where they left off. Offset 0 is the first message.
 * ============================================================================
 */
public class InMemoryLog {

    /**
     * The actual message storage: topic name → list of messages.
     *
     * ConcurrentHashMap: Thread-safe map. Multiple threads can read/write
     * without explicit synchronization. Used extensively in real Kafka.
     *
     * CopyOnWriteArrayList: Thread-safe list optimized for read-heavy workloads.
     * Every write creates a new copy of the underlying array (expensive for writes,
     * but reads never block). Perfect for our use case where consumers read far
     * more often than producers write.
     */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<byte[]>> logs = new ConcurrentHashMap<>();

    /**
     * Appends a message to the specified topic and returns the assigned offset.
     *
     * This is the core write operation — equivalent to what happens when a
     * Kafka producer sends a message. In real Kafka, this would:
     * 1. Validate the message format
     * 2. Assign an offset
     * 3. Append to the active segment file
     * 4. Update the offset index
     * 5. Notify followers to replicate
     *
     * We just do steps 2-3 in memory for now.
     *
     * @param topic   the topic name to write to
     * @param message the raw message bytes
     * @return the offset (position) where this message was stored
     */
    public long append(String topic, byte[] message) {
        // computeIfAbsent: atomically creates the list if this is the first
        // message for this topic. This is thread-safe — even if two threads
        // call this simultaneously for a new topic, only one list is created.
        CopyOnWriteArrayList<byte[]> topicLog = logs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());

        // Add the message to the end of the list.
        // The offset is simply the index in the list (0-based).
        topicLog.add(message);

        // Return the offset: list size - 1 (because we just added one)
        return topicLog.size() - 1;
    }

    /**
     * Reads messages from the specified topic starting at the given offset.
     *
     * This is the core read operation — equivalent to a Kafka consumer's FETCH.
     * In real Kafka, this would:
     * 1. Find the right segment file using the offset index
     * 2. Seek to the position in the file
     * 3. Read messages up to the max bytes limit
     * 4. Return messages up to the high watermark (replicated data only)
     *
     * @param topic       the topic name to read from
     * @param offset      the starting offset (inclusive)
     * @param maxMessages maximum number of messages to return
     * @return list of messages starting from the given offset (empty if topic doesn't exist)
     */
    public List<byte[]> read(String topic, long offset, int maxMessages) {
        CopyOnWriteArrayList<byte[]> topicLog = logs.get(topic);

        // Topic doesn't exist — return empty list (not null! Returning null
        // for collections is a common source of NullPointerExceptions)
        if (topicLog == null) {
            return Collections.emptyList();
        }

        int startIndex = (int) offset;
        int size = topicLog.size();

        // Offset is beyond what we have — no messages to return
        if (startIndex >= size) {
            return Collections.emptyList();
        }

        // Calculate end index, capped by maxMessages and available messages
        int endIndex = Math.min(startIndex + maxMessages, size);

        // subList returns a view, so we copy it to avoid ConcurrentModificationException
        // if the list is modified while the caller iterates over the result
        return new ArrayList<>(topicLog.subList(startIndex, endIndex));
    }

    /**
     * Returns the next offset that will be assigned for this topic.
     * This is the "end offset" — equivalent to Kafka's Log End Offset (LEO).
     *
     * In real Kafka, LEO is a critical metric:
     * - Producers need it to know if their message was stored
     * - Consumers use it to detect how far behind they are (consumer lag)
     * - Followers compare their LEO with the leader's to know if they're in-sync
     *
     * @param topic the topic name
     * @return the next offset (= number of messages in the topic)
     */
    public long getEndOffset(String topic) {
        CopyOnWriteArrayList<byte[]> topicLog = logs.get(topic);
        return topicLog != null ? topicLog.size() : 0;
    }

    /**
     * Checks if a topic exists in the log.
     * Used by request handlers to return UNKNOWN_TOPIC error if needed.
     */
    public boolean topicExists(String topic) {
        return logs.containsKey(topic);
    }

    /**
     * Creates a topic (empty log). In Phase 3, this will move to TopicManager
     * with partition support and persistent metadata.
     */
    public void createTopic(String topic) {
        logs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());
    }
}
