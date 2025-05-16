package com.danadler.kafka_bench;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class KafkaPipelineBenchmark {

    private static final int STAGE_COUNT = 10;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_PREFIX = "stage_";
    private static final long POLL_TIMEOUT_MS = 100;
    private static final int RECORD_COUNT = 100_000;
    private static final int RECORD_SIZE = 1000;

    private static final long[] stageEndTimestamps = new long[STAGE_COUNT];
    private static final long start_time=System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        createTopics();

        ExecutorService executor = Executors.newFixedThreadPool(STAGE_COUNT);

        for (int stage = 1; stage < STAGE_COUNT; stage++) {
            int finalStage = stage;
            executor.submit(() -> runStage(finalStage));
        }

        Semaphore inflight = new Semaphore(5000); // Limit to 5000 outstanding sends

        // Stage 0
        CountDownLatch latch = new CountDownLatch(RECORD_COUNT);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(baseProducerProps())) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                inflight.acquire(); // Block if too many outstanding sends

                String payload = generatePayload(RECORD_SIZE);
                producer.send(new ProducerRecord<>(TOPIC_PREFIX + "0", Integer.toString(i), payload),
                        (metadata, exception) -> {
                            inflight.release();
                            if (exception != null) exception.printStackTrace();
                            latch.countDown();
                        });
            }
            producer.flush();
            System.err.println("Stage 0: waiting for all sends...");
            latch.await();
            finalizeStage(0);
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);
    }

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existingTopics = admin.listTopics().names().get();
            List<NewTopic> topicsToCreate = new ArrayList<>();

            for (int i = 0; i < STAGE_COUNT; i++) {
                String topicName = TOPIC_PREFIX + i;
                if (!existingTopics.contains(topicName)) {
                    topicsToCreate.add(new NewTopic(topicName, 1, (short) 1));
                }
            }

            if (!topicsToCreate.isEmpty()) {
                admin.createTopics(topicsToCreate).all().get();
                System.err.printf("✅ Created %d new topic(s): %s%n", topicsToCreate.size(),
                        topicsToCreate.stream().map(NewTopic::name).toList());
            } else {
                System.err.println("✅ All Kafka topics already exist.");
            }
        }
    }

    private static void runStage(int stage) {
        String inputTopic = TOPIC_PREFIX + stage;
        String outputTopic = (stage < STAGE_COUNT - 1) ? TOPIC_PREFIX + (stage + 1) : null;
        CountDownLatch latch = new CountDownLatch(RECORD_COUNT);
        ExecutorService sendExecutor = Executors.newSingleThreadExecutor();

        Semaphore inflight = new Semaphore(5000); // per-stage backpressure

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseConsumerProps());
             KafkaProducer<String, String> producer = (outputTopic != null)
                     ? new KafkaProducer<>(baseProducerProps())
                     : null) {

            consumer.subscribe(Collections.singletonList(inputTopic));

            // Bootstrap polling until data is seen
            while (true) {
                ConsumerRecords<String, String> bootstrap = consumer.poll(Duration.ofMillis(500));
                if (!bootstrap.isEmpty()) {
                    System.err.printf("Stage %d: bootstrapped with %d records%n", stage, bootstrap.count());
                    break;
                }
            }

            int processed = 0;
            while (processed < RECORD_COUNT) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                for (ConsumerRecord<String, String> record : records) {
                    processed++;
                    final String key = record.key();
                    final String value = record.value();

                    if (outputTopic != null) {
                        sendExecutor.submit(() -> {
                            try {
                                inflight.acquire();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            producer.send(new ProducerRecord<>(outputTopic, key, value),
                                    (metadata, exception) -> {
                                        inflight.release();
                                        if (exception != null) exception.printStackTrace();
                                        latch.countDown();
                                    });
                        });
                    } else {
                        latch.countDown(); // last stage
                    }

                    if (processed >= RECORD_COUNT) break;
                }
            }

            if (producer != null) producer.flush();
            latch.await();
            System.err.printf("Stage %d: latch complete, finalizing%n", stage);
            finalizeStage(stage);
            sendExecutor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void finalizeStage(int stage) {
        try {
            if (stage > 0) {
                while (stageEndTimestamps[stage - 1] == 0) {
                    Thread.sleep(1);
                }
            }

            long now = System.currentTimeMillis();
            stageEndTimestamps[stage] = now;

            if (stage > 0 && now < stageEndTimestamps[stage - 1]) {
                throw new IllegalStateException("Stage " + stage + " ended at " + now +
                        ", before or equal to stage " + (stage - 1) + ": " + stageEndTimestamps[stage - 1]);
            }

            System.err.printf("Stage %d complete at %d%n", stage, now);

            if (stage == STAGE_COUNT - 1) {
                stageEndTimestamps[0] = start_time; // Stage 0 starts at the same time as the benchmark
                writeSummaryCsv();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Stage " + stage + " interrupted while waiting", e);
        }
    }

    private static void writeSummaryCsv() {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("benchmark_summary.csv")))) {
            writer.print("recordCount,recordSize");
            for (int i = 0; i < STAGE_COUNT; i++) {
                writer.print(",stage_" + i + "_end");
            }
            writer.println();

            writer.print(RECORD_COUNT + "," + RECORD_SIZE);
            for (int i = 0; i < STAGE_COUNT; i++) {
                writer.print("," + stageEndTimestamps[i]);
            }
            writer.println();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.err.println("✅ Benchmark summary written to benchmark_summary.csv");
    }

    private static String generatePayload(int size) {
        char[] chars = new char[size];
        Arrays.fill(chars, 'X');
        return new String(chars);
    }

    private static Properties baseConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "benchmark-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Properties baseProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 🔧 Throttling / backpressure
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);     // 64 MB
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);         // Wait max 10 sec if buffer full
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);           // 16 KB batches (default)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 25);               // Wait up to 25ms for batch

        return props;
    }
}