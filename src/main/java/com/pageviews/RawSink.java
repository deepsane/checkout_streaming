package com.pageviews;

import org.apache.kafka.clients.consumer.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public class RawSink {
    public static void main(String[] args) throws Exception {
        System.out.println("CWD: " + Paths.get("").toAbsolutePath());
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "raw-sink");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("pageviews"));
        Files.createDirectories(Paths.get("data/raw"));

        System.out.println("RawSink listening on 'pageviews'... writing to data/raw/");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Got record: " + record.value());
                String filename = "data/raw/" + record.partition() + "-" + record.offset() + ".json";
                Files.writeString(Paths.get(filename), record.value());
            }
        }
    }
}
