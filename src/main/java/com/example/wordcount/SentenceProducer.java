package com.example.wordcount;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
public class SentenceProducer {
    public static void main(String[] args) throws Exception {
        if(args.length < 1) { System.out.println("Usage: java SentenceProducer <file-path>"); return; }
        String filePath = args[0];
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<BOOTSTRAP_SERVERS>");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<>("sentences", line));
            }
        }
        producer.close();
        System.out.println("All sentences published to Kafka!");
    }
}