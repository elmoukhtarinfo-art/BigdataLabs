package edu.supmti.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class WordProducer {
    public static void main(String[] args) {
        if (args.length < 1) { System.out.println("Usage: WordProducer <topic>"); return; }
        String topic = args[0];

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        System.out.println("Tapez du texte (CTRL+C pour quitter):");
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            for (String word : line.split("\\s+")) {
                if(word.trim().isEmpty()) continue;
                producer.send(new ProducerRecord<>(topic, word));
                System.out.println("Envoyé: " + word);
            }
        }
        producer.close();
        scanner.close();
    }
}
