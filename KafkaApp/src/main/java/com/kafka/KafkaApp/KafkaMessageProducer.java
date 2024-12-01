package com.kafka.KafkaApp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaMessageProducer {
    public static void main(String[] args) {
        // Kafka broker address
        String bootstrapServers = "localhost:9092";
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter a Kafka Topic Name: ");
        String topicName = scanner.nextLine();

        // Create properties for AdminClient
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        int numberOfPartitions = 0;

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Describe the topic
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();

            // Get the topic description
            TopicDescription topicDescription = topicDescriptionMap.get(topicName);

            // Fetch the number of partitions
            numberOfPartitions = topicDescription.partitions().size();
            System.out.println("Number of partitions in topic '" + topicName + "': " + numberOfPartitions);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        // Set up producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try {
            // Send messages
            for (int i = 0; i < 10; i++) {
                String key = "key" + (i % numberOfPartitions);
                String value = "message" + (i + 1);

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, (Math.abs(key.hashCode()) + 1) % numberOfPartitions, key, value);
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the producer
            producer.close();
        }
    }
}
