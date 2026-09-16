package com.pageviews;

import org.apache.kafka.clients.producer.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Paths;
import java.util.*;

public class Producer {
    public static void main(String[] args) throws Exception {
        System.out.println("CWD: " + Paths.get("").toAbsolutePath());
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        String[] postcodes = {"SW19", "E1", "N1", "W2", "EC1"};
        String[] pages = {"/index.html", "/about", "/products", "/contact"};
        Random rnd = new Random();

        System.out.println("Producing pageview events to topic 'pageviews'... Ctrl+C to stop.");

        while (true) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("user_id", rnd.nextInt(5000) + 1);
            event.put("postcode", postcodes[rnd.nextInt(postcodes.length)]);
            event.put("webpage", "www.website.com" + pages[rnd.nextInt(pages.length)]);
            event.put("timestamp", System.currentTimeMillis() / 1000);

            String json = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>("pageviews", json), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send: " + exception.getMessage());
                } else {
                    System.out.println("Delivered to partition " + metadata.partition() + " offset " + metadata.offset());
                }
            });
            System.out.println("Sent: " + json);

            Thread.sleep(100 + rnd.nextInt(900));
        }
    }
}
