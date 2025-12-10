package edu.supmti.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {

    public static void main(String[] args) throws Exception {

        // Vérifier que le nom du topic est fourni
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0];

        // Configuration du Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

        // Création du producteur
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Envoi de 10 messages simples (clé=valeur)
        for (int i = 0; i < 10; i++) {
            producer.send(
                new ProducerRecord<String, String>(
                    topicName,
                    Integer.toString(i),
                    Integer.toString(i)
                )
            );
        }

        System.out.println("Messages envoyés avec succès !");
        producer.close();
    }
}
