# Streaming through processing stages 

This project is a generic benchmark on one machine (iMac Apple M3, 24GB mem, 8 cores) for comparing streaming data performance.

## Kafka Benchmark:

A payload of random characters (of specified length: 1000 bytes or 2000 bytes) is pushed through 10 stages of processing.

There are N (100k, 500k, 1M, 2M) rows in each benchmark run.

For simplicity, there is only one instance of the program running for all stages, and each stage runs on its own thread.

![Results](https://github.com/danadler-dev/stream_data_bench/blob/master/src/main/java/com/danadler/KafkaBenchmark.png)

## Kafka+Hazelcast Benchmark:

Same payload of random characters (of specified length: 1000 bytes or 2000 bytes) is pushed through 10 stages of processing.

There are N (100k, 500k, 1M, 2M) rows in each benchmark run.

This case is similar to the one above, except that the payload is not included in the Kafka message.

Instead, it is stored in a hazelcast distributed map.

For simplicity, there is only one instance of the program running for all stages, and each stage runs on its own thread.

However, in case the 10 stages were running in 10 different processes, each with a different processing step, we use tradeMap.executeOnKey() to update the data on the instance it exists in the distributed map.

This doesn't require managing a separate cluster of hazelcast nodes, and does not require sending the full payload back and forth over the network.

![Results](https://github.com/danadler-dev/stream_data_bench/blob/master/src/main/java/com/danadler/HazelBenchmark.png)

## Flink (local) Benchmark:

Same payload of random characters (of specified length: 1000 bytes or 2000 bytes) is pushed through 10 stages of processing.

There are N (100k, 500k, 1M, 2M, 10M, 100M) rows in each benchmark run.

In this case, we use flink locally (in process). There is no flink running separately.

We also use a SourceFunction<String> to emit one row of data at a time. In a real use case, this would be read from a file or a database or a kafka topic.

Since we never materialize the entire set of rows in memory, it can process 100M rows (or more) through the 10 stages on a single mac in less than a minute.

![Results](https://github.com/danadler-dev/stream_data_bench/blob/master/src/main/java/com/danadler/FlinkLocalBenchmark.png)


## Flink (cluster) Benchmark:

To run flink in a cluster, see the file under flink_bench/docker.

Once you run "docker compose up" you can see it in Docker Desktop.

Then in the terminal:
mvn clean package
docker cp target/stream_data_bench-1.0-SNAPSHOT.jar docker-jobmanager-1:/job.jar
docker exec -it docker-jobmanager-1 flink run /job.jar

That submits the job to flink. You can open localhost:8081 to see the flink dashboard that shows you the running, completed and failed jobs.

The following shows that running 2M records of data (1K bytes each) on a cluster with parallelism of 8 takes ~36 seconds:

![Results](https://github.com/danadler-dev/stream_data_bench/blob/master/src/main/java/com/danadler/Flink-Cluster-8-2M-1K.png)

The following shows that running 10M records of data (1K bytes each) on a cluster with parallelism of 8 takes ~3 minutes:

![Results](https://github.com/danadler-dev/stream_data_bench/blob/master/src/main/java/com/danadler/Flink-Cluster-8-10M-1K.png)

Larger results are hard to obtain on a single machine with limited memory and cpu.

If you compare flink on a cluster to flink running in process locally, it appears that the in-process version runs much faster.

This is because it doesn't have any network overhead and does not push the data from process to process. In fact, it only pushes one row at a time, which explains why it's easy to process 100M rows in a local flink process, but not in the docker cluster running on the same machine.

