package com.kafka.KafkaApp;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaAdmin {
    public static void main(String[] args) {
        // Set up AdminClient properties
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Scanner scanner = new Scanner(System.in);

        // Create AdminClient
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Take user input for the kafka topic
            System.out.print("Enter a Kafka Topic Name: ");
            String topic =  scanner.nextLine();

            // Take user input for number of partitions
            System.out.print("Enter Number of Partitions: ");
            int numPartitions = scanner.nextInt();

            // Take user input for replication factor
            System.out.print("Enter Replication Factor: ");
            short replicationFactor = scanner.nextShort();

            // Create the topic if it doesn't exist
            NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
