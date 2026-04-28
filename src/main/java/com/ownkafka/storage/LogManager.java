package com.ownkafka.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * ============================================================================
 * LogManager — Top-level façade for all topics' commit logs.
 * ============================================================================
 *
 * WHAT: This is the class our handlers (ProduceHandler, FetchHandler) use.
 *       It manages many CommitLog instances — one per topic — and handles:
 *       - Topic creation
 *       - Routing append/read calls to the right CommitLog
 *       - Startup recovery (scan data dir for existing topics)
 *       - Background retention enforcement (every 30s)
 *       - Coordinated shutdown (flush + close all topics)
 *
 * WHY A FAÇADE?
 *       Handlers shouldn't know about segments, indexes, or files. They just
 *       want "append this message" and "read messages from offset X."
 *       LogManager presents that simple API while hiding all the complexity.
 *
 *       This is the same role InMemoryLog played in Phase 1 — same API,
 *       different implementation behind it. Drop-in replacement.
 *
 * BACKGROUND RETENTION:
 *       A ScheduledExecutorService runs enforceRetentionAll() every 30s.
 *       This deletes old segments to keep disk usage bounded.
 *       Real Kafka uses log.retention.check.interval.ms (default 5 min).
 *
 * SHUTDOWN ORDER:
 *       1. Cancel the retention scheduler
 *       2. Flush all topics (force fsync)
 *       3. Close all CommitLogs (which closes all Segments)
 *       Handlers should stop accepting new requests BEFORE close() is called,
 *       otherwise they'll hit closed segments. The shutdown hook in
 *       OwnKafkaServer does this in the right order.
 *
 * INTERVIEW TIP: "How does Kafka handle startup with millions of segments?"
 *       → On startup, Kafka scans the log directory and reopens each segment.
 *         Recovery is parallelized across multiple threads. Each segment
 *         validates its index, runs the recovery scan if it was the last
 *         active one (which might have torn writes), and registers itself.
 *         A cluster with 1000 partitions × 100 segments = 100k segments
 *         can recover in seconds because most segments are clean.
 * ============================================================================
 */
public final class LogManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);

    /** Default frequency at which retention is enforced. */
    private static final long RETENTION_INTERVAL_MS = 30_000;

    /** Default fetch batch size when read() is called. */
    private static final int DEFAULT_FETCH_MAX_BYTES = 1 * 1024 * 1024; // 1 MB

    private final LogConfig config;
    private final ConcurrentHashMap<String, CommitLog> logs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService retentionScheduler;
    private volatile boolean closed = false;

    public LogManager(LogConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.dataDir());

        // Recover all existing topics
        recoverAllTopics();

        // Schedule periodic retention checks
        this.retentionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("log-retention-scheduler");
            t.setDaemon(true);
            return t;
        });
        retentionScheduler.scheduleAtFixedRate(
                this::enforceRetentionSafely,
                RETENTION_INTERVAL_MS,
                RETENTION_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        logger.info("LogManager started. dataDir={}, recovered topics={}",
                config.dataDir(), logs.keySet());
    }

    /**
     * Walk the data directory, treating each subdirectory as a topic to recover.
     */
    private void recoverAllTopics() throws IOException {
        try (Stream<Path> stream = Files.list(config.dataDir())) {
            stream
                    .filter(Files::isDirectory)
                    .forEach(topicDir -> {
                        String topic = topicDir.getFileName().toString();
                        try {
                            CommitLog log = CommitLog.openOrCreate(topic, config);
                            logs.put(topic, log);
                            logger.info("Recovered topic '{}' (endOffset={})", topic, log.endOffset());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to recover topic " + topic, e);
                        }
                    });
        }
    }

    /**
     * Append a message to a topic. Auto-creates the topic if it doesn't exist.
     * Returns the offset assigned to the message.
     */
    public long append(String topic, byte[] message) throws IOException {
        ensureNotClosed();
        CommitLog log = logs.computeIfAbsent(topic, t -> {
            try {
                CommitLog newLog = CommitLog.openOrCreate(t, config);
                logger.info("Auto-created topic '{}'", t);
                return newLog;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create topic " + t, e);
            }
        });
        return log.append(message);
    }

    /**
     * Read up to maxMessages from a topic starting at the given offset.
     * Returns an empty list if the topic doesn't exist or no messages from offset.
     */
    public List<byte[]> read(String topic, long offset, int maxMessages) throws IOException {
        ensureNotClosed();
        CommitLog log = logs.get(topic);
        if (log == null) {
            return List.of();
        }
        List<LogRecord> records = log.read(offset, maxMessages, DEFAULT_FETCH_MAX_BYTES);
        List<byte[]> result = new ArrayList<>(records.size());
        for (LogRecord r : records) {
            result.add(r.value());
        }
        return result;
    }

    /** End offset of a topic (next offset to be assigned). */
    public long getEndOffset(String topic) {
        CommitLog log = logs.get(topic);
        return (log != null) ? log.endOffset() : 0L;
    }

    public boolean topicExists(String topic) {
        return logs.containsKey(topic);
    }

    /** Explicitly create a topic (no-op if it already exists). */
    public void createTopic(String topic) throws IOException {
        ensureNotClosed();
        logs.computeIfAbsent(topic, t -> {
            try {
                return CommitLog.openOrCreate(t, config);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create topic " + t, e);
            }
        });
    }

    /** Run retention on every topic. Called by the scheduler. */
    public void enforceRetentionAll() throws IOException {
        for (CommitLog log : logs.values()) {
            int deleted = log.enforceRetention();
            if (deleted > 0) {
                logger.info("Retention deleted {} segments from topic '{}'", deleted, log.topic());
            }
        }
    }

    /** Wrapper that catches and logs IO errors so the scheduler doesn't die. */
    private void enforceRetentionSafely() {
        try {
            enforceRetentionAll();
        } catch (Exception e) {
            logger.error("Retention check failed", e);
        }
    }

    /** Force fsync on all topics. */
    public void flushAll() throws IOException {
        for (CommitLog log : logs.values()) {
            log.flush();
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("LogManager is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;

        // Stop the scheduler
        retentionScheduler.shutdownNow();
        try {
            retentionScheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Flush + close every commit log
        IOException firstError = null;
        for (CommitLog log : logs.values()) {
            try {
                log.flush();
                log.close();
            } catch (IOException e) {
                logger.warn("Error closing topic '{}': {}", log.topic(), e.getMessage());
                if (firstError == null) firstError = e;
            }
        }
        logs.clear();

        logger.info("LogManager closed");
        if (firstError != null) throw firstError;
    }
}
