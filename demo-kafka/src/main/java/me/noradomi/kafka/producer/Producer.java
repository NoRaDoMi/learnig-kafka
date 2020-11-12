package me.noradomi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
  public static void main(String[] args) {

    String bootstrapServers = "127.0.0.1:9092";

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    kafkaProps.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaProps.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

    ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello Noradomi 2");

//    async action
    producer.send(record);

//  force wait for sent
    producer.flush();

    producer.close();
  }
}
