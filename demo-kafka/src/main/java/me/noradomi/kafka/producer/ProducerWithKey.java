package me.noradomi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {
  public static void main(String[] args) throws InterruptedException, ExecutionException {

    Logger log = LoggerFactory.getLogger(ProducerWithKey.class.getName());

    String bootstrapServers = "127.0.0.1:9092";

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    kafkaProps.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaProps.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

    for (int i = 0; i < 10; i++) {
      String topic = "first_topic";
      String key = "id_" + i;
      String value = "Hello " + i;

      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

      log.info("Key : " + key);
      //    async action
      producer
          .send(
              record,
              (recordMetadata, e) -> {
                if (e == null) {
                  log.info(
                      "Received new metadata.\n"
                          + "Topic: "
                          + recordMetadata.topic()
                          + "\n"
                          + "Partition: "
                          + recordMetadata.partition()
                          + "\n"
                          + "Offset: "
                          + recordMetadata.offset()
                          + "\n"
                          + "Timestamp: "
                          + recordMetadata.timestamp());
                } else {
                  log.error("Error while producing", e);
                }
              })
          .get();
    }

    //  force wait for sent
    producer.flush();

    producer.close();
  }
}
