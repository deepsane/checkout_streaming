# Real-Time Pageview Pipeline

## Architecture
Producer → Kafka topic `pageviews` → two independent consumers:
- **RawSink**: writes each event untouched to `data/raw/` (one file per record)
- **Aggregator**: tumbling 1-minute windows, counts pageviews per postcode, writes to `data/aggregates/`

## Stack
- Apache Kafka (KRaft mode, single broker, via Docker — no Zookeeper needed)
- Java 17, Maven
- kafka-clients + Jackson (JSON)

## Run

```bash
docker-compose up -d
mvn clean package
java -cp target/pipeline.jar com.pageviews.Producer
java -cp target/pipeline.jar com.pageviews.RawSink
java -cp target/pipeline.jar com.pageviews.Aggregator
```

Run each in a separate terminal. Output appears in `data/raw/` and `data/aggregates/`
within ~60 seconds of the producer starting.

## Design decisions

- **Plain `KafkaConsumer` over Kafka Streams**: given the time-boxed scope, a
  hand-rolled tumbling window was faster to build and verify than standing up
  the Streams DSL. In a production/non-time-boxed version, I'd use Kafka
  Streams' `TimeWindows` with a configurable grace period for proper
  watermarking of late data.
- **At-least-once delivery**: acceptable for this exercise; no dedup/idempotency
  handling implemented.
- **Late/out-of-order events**: not specifically handled — an event lands in
  whichever window is "current" when the consumer processes it. A production
  version would use Kafka Streams' windowed state store with a grace period.
- **users stream**: out of scope per requirements — `user_id` is a passthrough
  field only, no join performed against a users stream.
- **Output format**: JSON, one file per flush/record. Chosen for readability
  over Parquet given the small (<100K events/day) data volume stated in the brief.
- **Partitioning**: single partition topic, single broker — sufficient for
  <100K events/day; would partition by postcode for horizontal scaling at
  higher volume.
- **Sample data**: `Producer.java` generates synthetic pageview events at
  random intervals (100ms–1s) across a fixed set of sample postcodes and pages.

## Known limitations
- No graceful shutdown flush (in-flight aggregation window is lost on kill)
- No exactly-once semantics
- No schema validation/registry (e.g. Avro + Schema Registry) — plain JSON
- Single broker, no replication — not fault-tolerant

## Project structure
```
.
├── docker-compose.yml
├── pom.xml
├── README.md
├── src/main/java/com/pageviews/
│   ├── Producer.java
│   ├── RawSink.java
│   └── Aggregator.java
└── data/
    ├── raw/
    └── aggregates/
```
