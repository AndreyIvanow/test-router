package ru.vtb.router;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.vtb.common.Constants;
import ru.vtb.zf.dto.ExchangeDeal;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(4);

        Properties shardConsumerProps = new Properties();
        shardConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "SHARD_CONSUMER_GROUP");
        shardConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        shardConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        shardConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        for (int i = 0; i < 4; i++) {
            int partition = i;
            Runnable runnable = () -> {
                try (KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(shardConsumerProps)) {
                    TopicPartition tp = new TopicPartition(Constants.ROUTER_TO_SHARDS_TOPIC, partition);
                    consumer.assign(Arrays.asList(tp));
                    consumer.seekToBeginning(Arrays.asList(tp));
                    System.out.println(String.format("Starting consuming from partition № %s", partition));
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.registerModule(new JavaTimeModule());
                    while (true) {
                        ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                        if (!consumerRecords.isEmpty()) {
                            for (var rec : consumerRecords) {
                                System.out.println(String.format("Received: topic == [%s], partition == [%s], offset == [%s], key == [%s], value == [%s]",
                                        rec.topic(), rec.partition(), rec.offset(), rec.key(), rec.value()));
                                try {
                                    ExchangeDeal exchangeDeal = objectMapper.readValue(rec.value(), ExchangeDeal.class);
                                    TimeUnit.MILLISECONDS.sleep(100);
                                } catch (JsonProcessingException e) {
                                    e.printStackTrace();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            };
            executorService.submit(runnable);
        }



        executorService.shutdown();

    }

}
