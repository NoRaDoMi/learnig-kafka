package me.noradomi.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoAssignSeek {

  public static void main(String[] args) {
    new ConsumerDemoAssignSeek().run();
  }

  private ConsumerDemoAssignSeek() {}

  private void run() {
    Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

    String bootstrapServers = "localhost:9092";
    String topic = "first_topic";

    CountDownLatch latch = new CountDownLatch(1);

    log.info("Creating the consumer thread");
    ConsumerRunnale myConsumerThread = new ConsumerRunnale(latch, bootstrapServers, topic);

    Thread myThread = new Thread(myConsumerThread);
    myThread.start();

    //  add a shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Caught shutdown hook");
                  myConsumerThread.shutdown();
                }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      log.error("Application got interrupted", e);
    } finally {
      log.info("Application is closing");
    }
  }

  public class ConsumerRunnale implements Runnable {
    private final Logger log = LoggerFactory.getLogger(ConsumerRunnale.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnale(CountDownLatch latch, String bootstrapServers, String topic) {
      this.latch = latch;

      //    create consumer configs
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      //    create consumer
      consumer = new KafkaConsumer<String, String>(properties);

      //     assign and seek are mostly used to replay data or fetch a specific message

      //      assign


      //    subscribe topics
      consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
      //    polling loop
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            log.info("Key: " + record.key() + ", Value: " + record.value());
            log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
          }
        }
      } catch (WakeupException e) {
        log.info("Received shutdown signal");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutdown() {
      //      wakeup method will interrupt consumer.poll() => throw WakeUpConsumer
      consumer.wakeup();
    }
  }
}
