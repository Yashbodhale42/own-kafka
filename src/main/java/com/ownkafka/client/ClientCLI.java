package com.ownkafka.client;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * ============================================================================
 * ClientCLI — Interactive command-line client to test our Kafka broker.
 * ============================================================================
 *
 * WHAT: A simple interactive CLI that lets you produce and fetch messages
 *       from the command line. Similar to Kafka's built-in tools:
 *       - kafka-console-producer.sh
 *       - kafka-console-consumer.sh
 *
 * HOW TO USE:
 *       1. Start the broker:  ./mvnw exec:java -Dexec.mainClass=com.ownkafka.OwnKafkaServer
 *       2. Start the client:  ./mvnw exec:java -Dexec.mainClass=com.ownkafka.client.ClientCLI
 *
 * COMMANDS:
 *       produce <topic> <message>  — Send a message to a topic
 *       fetch <topic> <offset>     — Read messages from a topic starting at offset
 *       help                       — Show available commands
 *       exit                       — Disconnect and quit
 *
 * REAL KAFKA CLI TOOLS:
 *       kafka-console-producer --bootstrap-server localhost:9092 --topic test
 *       kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
 *
 *       In Phase 5, when we implement the real Kafka wire protocol,
 *       you'll be able to use the ACTUAL Kafka CLI tools against our broker!
 * ============================================================================
 */
public class ClientCLI {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9092;

    public static void main(String[] args) {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        // Allow host:port override via command line
        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[1]);
                return;
            }
        }

        System.out.println("============================================");
        System.out.println("     OWN KAFKA CLIENT — Phase 1 CLI");
        System.out.println("============================================");
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (OwnKafkaClient client = new OwnKafkaClient(host, port);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected! Type 'help' for available commands.\n");

            while (true) {
                System.out.print("own-kafka> ");
                String line = scanner.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

                // Split into command and arguments (let handlers re-split as needed)
                String[] firstSplit = line.split("\\s+", 2);
                String command = firstSplit[0].toLowerCase();
                String rest = firstSplit.length > 1 ? firstSplit[1] : "";

                try {
                    switch (command) {
                        case "produce" -> handleProduce(client, rest);
                        case "fetch" -> handleFetch(client, rest);
                        case "create-topic" -> handleCreateTopic(client, rest);
                        case "delete-topic" -> handleDeleteTopic(client, rest);
                        case "metadata" -> handleMetadata(client, rest);
                        case "help" -> printHelp();
                        case "exit", "quit" -> {
                            System.out.println("Goodbye!");
                            return;
                        }
                        default -> System.out.println("Unknown command: " + command
                                + ". Type 'help' for available commands.");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            System.err.println("Failed to connect to broker: " + e.getMessage());
            System.err.println("Make sure the broker is running on " + host + ":" + port);
        }
    }

    /**
     * Handles the 'produce' command.
     *
     * Usage (3 forms):
     *   produce <topic> <message>                  → v0 path, partition 0
     *   produce <topic> --key=<key> <message>      → v1, key-based routing
     *   produce <topic> --partition=<n> <message>  → v1, explicit partition
     */
    private static void handleProduce(OwnKafkaClient client, String rest) throws IOException {
        if (rest.isBlank()) {
            System.out.println("Usage: produce <topic> [--key=<key> | --partition=<n>] <message>");
            return;
        }
        String[] parts = rest.split("\\s+", 2);
        String topic = parts[0];
        if (parts.length < 2) {
            System.out.println("Missing message. Usage: produce <topic> [--key=<key> | --partition=<n>] <message>");
            return;
        }
        String remainder = parts[1].trim();

        // Check for --key=... or --partition=... prefix
        if (remainder.startsWith("--key=")) {
            String[] kv = remainder.split("\\s+", 2);
            if (kv.length < 2) {
                System.out.println("Missing message after --key=...");
                return;
            }
            String key = kv[0].substring("--key=".length());
            String message = kv[1];
            OwnKafkaClient.ProduceResult result = client.produce(topic, key, message);
            System.out.printf("Produced to '%s' (%s) [key=%s]%n", topic, result, key);
        } else if (remainder.startsWith("--partition=")) {
            String[] kv = remainder.split("\\s+", 2);
            if (kv.length < 2) {
                System.out.println("Missing message after --partition=...");
                return;
            }
            int partition;
            try {
                partition = Integer.parseInt(kv[0].substring("--partition=".length()));
            } catch (NumberFormatException e) {
                System.out.println("Invalid partition number: " + kv[0]);
                return;
            }
            String message = kv[1];
            OwnKafkaClient.ProduceResult result = client.produceToPartition(topic, partition, message);
            System.out.printf("Produced to '%s' (%s)%n", topic, result);
        } else {
            // v0 path
            long offset = client.produce(topic, remainder);
            System.out.printf("Produced to '%s' offset=%d (v0, partition 0)%n", topic, offset);
        }
    }

    /**
     * Handles the 'fetch' command.
     *
     * Usage:
     *   fetch <topic> <offset>                → v0, partition 0
     *   fetch <topic> <partition> <offset>    → v1
     */
    private static void handleFetch(OwnKafkaClient client, String rest) throws IOException {
        if (rest.isBlank()) {
            System.out.println("Usage: fetch <topic> [<partition>] <offset>");
            return;
        }
        String[] parts = rest.split("\\s+");

        List<OwnKafkaClient.FetchedMessage> messages;
        String topic;
        long offset;

        if (parts.length == 2) {
            // v0: fetch <topic> <offset>
            topic = parts[0];
            try {
                offset = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid offset: " + parts[1]);
                return;
            }
            messages = client.fetch(topic, offset);
            System.out.printf("Fetched %d msg(s) from '%s' (v0, partition 0):%n", messages.size(), topic);
        } else if (parts.length == 3) {
            // v1: fetch <topic> <partition> <offset>
            topic = parts[0];
            int partition;
            try {
                partition = Integer.parseInt(parts[1]);
                offset = Long.parseLong(parts[2]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid partition/offset: " + parts[1] + " " + parts[2]);
                return;
            }
            messages = client.fetch(topic, partition, offset);
            System.out.printf("Fetched %d msg(s) from '%s' partition=%d:%n", messages.size(), topic, partition);
        } else {
            System.out.println("Usage: fetch <topic> [<partition>] <offset>");
            return;
        }

        for (OwnKafkaClient.FetchedMessage msg : messages) {
            System.out.printf("  [offset %d] %s%n", msg.offset(), msg.dataAsString());
        }
    }

    private static void handleCreateTopic(OwnKafkaClient client, String rest) throws IOException {
        String[] parts = rest.trim().split("\\s+");
        if (parts.length < 2) {
            System.out.println("Usage: create-topic <name> <numPartitions>");
            return;
        }
        String topic = parts[0];
        int numPartitions;
        try {
            numPartitions = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid partition count: " + parts[1]);
            return;
        }
        client.createTopic(topic, numPartitions);
        System.out.printf("Created topic '%s' with %d partitions%n", topic, numPartitions);
    }

    private static void handleDeleteTopic(OwnKafkaClient client, String rest) throws IOException {
        String topic = rest.trim();
        if (topic.isEmpty()) {
            System.out.println("Usage: delete-topic <name>");
            return;
        }
        client.deleteTopic(topic);
        System.out.printf("Deleted topic '%s'%n", topic);
    }

    private static void handleMetadata(OwnKafkaClient client, String rest) throws IOException {
        List<String> topics = rest.isBlank()
                ? List.of()
                : List.of(rest.trim().split("\\s+"));

        com.ownkafka.protocol.ProtocolCodec.MetadataResponse meta = client.metadata(topics);

        System.out.println("Brokers:");
        for (var b : meta.brokers()) {
            System.out.printf("  [%d] %s:%d%n", b.id(), b.host(), b.port());
        }
        System.out.println("Topics:");
        if (meta.topics().isEmpty()) {
            System.out.println("  (none)");
        }
        for (var t : meta.topics()) {
            System.out.printf("  %s (%d partitions)%n", t.topic(), t.partitions().size());
            for (var p : t.partitions()) {
                System.out.printf("    partition %d  →  leader broker %d%n",
                        p.partition(), p.leaderBrokerId());
            }
        }
    }

    private static void printHelp() {
        System.out.println("""

                Available commands:
                  produce <topic> <message>                       — v0: send to partition 0
                  produce <topic> --key=<key> <message>           — v1: route by key hash
                  produce <topic> --partition=<n> <message>       — v1: explicit partition
                  fetch <topic> <offset>                          — v0: fetch from partition 0
                  fetch <topic> <partition> <offset>              — v1: fetch from specific partition
                  create-topic <name> <numPartitions>             — Create a new topic
                  delete-topic <name>                             — Delete a topic
                  metadata                                        — List all topics
                  metadata <topic> [topic...]                     — List specific topics
                  help
                  exit

                Examples:
                  create-topic orders 3
                  produce orders --key=customer-42 "Pizza order"
                  produce orders --partition=1 "Burger order"
                  metadata orders
                  fetch orders 0 0
                  fetch orders 1 0
                  delete-topic orders
                """);
    }
}
