package com.pageviews;

import org.apache.kafka.clients.consumer.*;
import com.fasterxml.jackson.databind.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public class Aggregator {

    static long windowStart(long ts, long size) {
        return ts - (ts % size);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("CWD: " + Paths.get("").toAbsolutePath());
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "aggregator");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("pageviews"));
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Integer> counts = new HashMap<>(); // key: postcode
        long currentWindow = -1;
        Files.createDirectories(Paths.get("data/aggregates"));

        System.out.println("Aggregator listening on 'pageviews'... writing 1m windows to data/aggregates/");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Got record: " + record.value());
                JsonNode event = mapper.readTree(record.value());
                long ts = event.get("timestamp").asLong();
                long w = windowStart(ts, 60);
                String postcode = event.get("postcode").asText();

                if (currentWindow == -1) {
                    currentWindow = w;
                }
                if (w > currentWindow) {
                    flush(mapper, currentWindow, counts);
                    counts.clear();
                    currentWindow = w;
                }
                counts.merge(postcode, 1, Integer::sum);
            }
        }
    }

    static void flush(ObjectMapper mapper, long window, Map<String, Integer> counts) throws Exception {
        if (counts.isEmpty()) return;
        List<Map<String, Object>> rows = new ArrayList<>();
        for (var e : counts.entrySet()) {
            rows.add(Map.of(
                "postcode", e.getKey(),
                "window_start", window,
                "count", e.getValue()
            ));
        }
        Files.writeString(Paths.get("data/aggregates/" + window + ".json"), mapper.writeValueAsString(rows));
        System.out.println("Flushed window " + window + ": " + rows);
    }
}
