# own-kafka

Building Apache Kafka from scratch in Java — a hands-on deep dive into distributed event streaming.

This project re-implements core Kafka concepts phase by phase: TCP networking, binary wire protocol, commit log storage, topic partitioning, consumer groups, replication, and more.

---

## Why This Project?

Reading about Kafka is one thing. Building it is another.

This project exists to deeply understand *how* Kafka works under the hood — not just how to use it. Every design decision, every byte on the wire, every thread — written from scratch with the goal of understanding the real thing.

---

## Phases

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | TCP Server + Binary Protocol + In-Memory Storage | ✅ Complete |
| 2 | Disk-based Commit Log (Segment files, Offset Index) | 🔜 Next |
| 3 | Topics & Partitions | ⏳ Planned |
| 4 | Consumer Groups & Offset Tracking | ⏳ Planned |
| 5 | Real Kafka Wire Protocol (connect real Kafka CLI tools) | ⏳ Planned |
| 6 | Multi-Broker Cluster | ⏳ Planned |
| 7 | Replication (ISR, High Watermark, Leader Failover) | ⏳ Planned |
| 8 | Advanced (Log Compaction, ACLs, Idempotent Producer) | ⏳ Planned |

---

## Phase 1 — What's Built

A working TCP broker that speaks a custom binary protocol:

```
Client (CLI)
  ↓  TCP — length-prefixed binary frames
BrokerServer  (thread-per-connection, port 9092)
  ↓
RequestHandler  (routes by API key)
  ↓
ProduceHandler / FetchHandler
  ↓
InMemoryLog  (ConcurrentHashMap + CopyOnWriteArrayList)
```

**Custom wire format:**
```
Request:  [length:4][apiKey:2][apiVersion:2][correlationId:4][payload...]
Response: [length:4][correlationId:4][errorCode:2][payload...]
```

**Supported operations:**
- `PRODUCE` (API key 0) — write a message to a topic, get back the offset
- `FETCH` (API key 1) — read messages from a topic starting at an offset

---

## Requirements

- Java 23 — [Download JDK 23](https://www.oracle.com/java/technologies/downloads/)
- No Maven install needed — the Maven wrapper (`mvnw`) handles it automatically

---

## Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/Yashbodhale42/own-kafka.git
cd own-kafka

# 2. Set JAVA_HOME (Windows)
set JAVA_HOME=C:\Program Files\Java\jdk-23

# 3. Compile
./mvnw.cmd compile

# 4. Run tests
./mvnw.cmd test
```

---

## Running the Broker

**Terminal 1 — Start the broker:**
```bash
./mvnw.cmd exec:java -Dexec.mainClass=com.ownkafka.OwnKafkaServer
```

**Terminal 2 — Start the CLI client:**
```bash
./mvnw.cmd exec:java -Dexec.mainClass=com.ownkafka.client.ClientCLI
```

**Example session:**
```
own-kafka> produce orders "New order from customer 42"
Message produced to topic 'orders' at offset 0

own-kafka> produce orders "Another order"
Message produced to topic 'orders' at offset 1

own-kafka> fetch orders 0
Fetched 2 message(s) from topic 'orders':
  [offset 0] New order from customer 42
  [offset 1] Another order

own-kafka> exit
```

---

## Running the Integration Test

```bash
./mvnw.cmd compile test-compile
java -ea -cp "target/classes;target/test-classes;$(cat target/cp.txt)" com.ownkafka.IntegrationTest
```

This starts a broker in-process, connects a client, produces and fetches messages across multiple topics, and asserts correctness end-to-end.

---

## Project Structure

```
src/main/java/com/ownkafka/
├── OwnKafkaServer.java          # Main entry point
├── protocol/
│   ├── ApiKeys.java             # PRODUCE(0), FETCH(1)
│   ├── ErrorCode.java           # NONE(0), UNKNOWN_TOPIC(3), ...
│   ├── RequestHeader.java       # apiKey + apiVersion + correlationId
│   ├── Request.java             # header + payload
│   ├── Response.java            # correlationId + errorCode + payload
│   └── ProtocolCodec.java       # Binary encode/decode
├── storage/
│   └── InMemoryLog.java         # Thread-safe message store
├── server/
│   ├── BrokerServer.java        # TCP accept loop + thread pool
│   ├── ClientSession.java       # Per-connection framing state
│   └── RequestHandler.java      # Routes by API key
├── handler/
│   ├── ProduceHandler.java      # Handles PRODUCE requests
│   └── FetchHandler.java        # Handles FETCH requests
└── client/
    ├── OwnKafkaClient.java      # TCP client library
    └── ClientCLI.java           # Interactive CLI
```

---

## Key Concepts Implemented

**Length-prefixed message framing**
TCP is a stream — it has no concept of message boundaries. Every frame starts with a 4-byte integer telling the receiver exactly how many bytes to read next.

**Binary protocol**
All data is encoded as raw bytes (integers as 4 bytes, strings as length + UTF-8 bytes). More compact and faster to parse than text-based protocols like HTTP/1.1 or JSON.

**Correlation IDs**
Every request carries a unique integer ID. The broker echoes it in the response, allowing the client to match responses to requests even when multiple are in-flight.

**API versioning**
Every request header includes an `apiVersion` field. This enables backward compatibility — old clients can talk to new brokers, which is how Kafka achieves rolling upgrades with zero downtime.

**Thread-safe in-memory log**
`ConcurrentHashMap` for per-topic storage, `CopyOnWriteArrayList` for the message list. Allows concurrent producers and consumers without explicit locking.

---

## Tests

```
src/test/java/com/ownkafka/
├── protocol/ProtocolCodecTest.java    # Encode/decode roundtrip tests
├── storage/InMemoryLogTest.java       # Storage correctness + concurrency
├── server/RequestHandlerTest.java     # Produce/fetch end-to-end logic
└── IntegrationTest.java               # Full broker + client test
```

Run all tests:
```bash
./mvnw.cmd test
```

Expected output:
```
Tests run: 24, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Java | 23 | Core language (records, pattern matching, sealed classes) |
| Maven | 3.9.6 (via wrapper) | Build tool |
| JUnit 5 | 5.10.2 | Unit testing |
| SLF4J + Logback | 1.4.14 | Structured logging |

---

## What's Next — Phase 2

Replace the in-memory store with a real disk-based commit log:

- Append-only `.log` segment files (like real Kafka)
- Sparse `.index` files for fast offset lookups
- Segment rolling when a segment exceeds a size limit
- Time/size-based log retention (auto-delete old segments)
- Recovery on startup by scanning existing segment files

Messages will survive server restarts.

---

## License

MIT
