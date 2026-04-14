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

                // Split into command and arguments
                String[] parts = line.split("\\s+", 3); // max 3 parts: command, topic, message
                String command = parts[0].toLowerCase();

                try {
                    switch (command) {
                        case "produce" -> handleProduce(client, parts);
                        case "fetch" -> handleFetch(client, parts);
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
     * Usage: produce <topic> <message>
     */
    private static void handleProduce(OwnKafkaClient client, String[] parts) throws IOException {
        if (parts.length < 3) {
            System.out.println("Usage: produce <topic> <message>");
            System.out.println("Example: produce orders \"New order from customer 42\"");
            return;
        }

        String topic = parts[1];
        String message = parts[2];

        long offset = client.produce(topic, message);
        System.out.printf("Message produced to topic '%s' at offset %d%n", topic, offset);
    }

    /**
     * Handles the 'fetch' command.
     * Usage: fetch <topic> <offset>
     */
    private static void handleFetch(OwnKafkaClient client, String[] parts) throws IOException {
        if (parts.length < 3) {
            System.out.println("Usage: fetch <topic> <offset>");
            System.out.println("Example: fetch orders 0");
            return;
        }

        String topic = parts[1];
        long offset;
        try {
            offset = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid offset: " + parts[2] + " (must be a number)");
            return;
        }

        List<OwnKafkaClient.FetchedMessage> messages = client.fetch(topic, offset);

        if (messages.isEmpty()) {
            System.out.println("No messages found at offset " + offset + " in topic '" + topic + "'");
        } else {
            System.out.printf("Fetched %d message(s) from topic '%s':%n", messages.size(), topic);
            for (OwnKafkaClient.FetchedMessage msg : messages) {
                System.out.printf("  [offset %d] %s%n", msg.offset(), msg.dataAsString());
            }
        }
    }

    private static void printHelp() {
        System.out.println("""

                Available commands:
                  produce <topic> <message>  — Send a message to a topic
                  fetch <topic> <offset>     — Read messages starting from offset
                  help                       — Show this help message
                  exit                       — Disconnect and quit

                Examples:
                  produce orders "New order from customer 42"
                  produce logs "Server started at 10:00 AM"
                  fetch orders 0
                  fetch logs 0
                """);
    }
}
