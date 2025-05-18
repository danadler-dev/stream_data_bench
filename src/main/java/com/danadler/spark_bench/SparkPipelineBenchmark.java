package com.danadler.spark_bench;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import java.io.*;
import java.util.*;

public class SparkPipelineBenchmark {
    private static final int RECORD_COUNT = 4_000_000;
    private static final int RECORD_SIZE = 1000;
    private static final int STAGE_COUNT = 10;
    private static final long[] stageEndTimestamps = new long[STAGE_COUNT];
    private static final long startTime = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("SparkPipelineBenchmark")
                .setMaster("spark://spark-master:7077")
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "4g");

        JavaSparkContext sc = new JavaSparkContext(conf);
        stageEndTimestamps[0] = startTime;

        String payload = "X".repeat(RECORD_SIZE);
        JavaRDD<String> rdd = sc
                .parallelize(Arrays.asList(new String[RECORD_COUNT]), sc.defaultParallelism())
                .map(x -> payload);

        // Apply all stages
        for (int i = 0; i < STAGE_COUNT; i++) {
            final int stage = i;
            LongAccumulator acc = sc.sc().longAccumulator("stage_" + stage + "_count");

            rdd = rdd.mapPartitions((FlatMapFunction<Iterator<String>, String>) iter -> {
                List<String> out = new ArrayList<>();
                while (iter.hasNext()) {
                    String in = iter.next();
                    acc.add(1);
                    out.add(String.valueOf((char) ('0' + stage)).repeat(in.length()));
                }

                return out.iterator();
            });

            // Force materialization after each stage
            long thisStageCount = rdd.count();
            stageEndTimestamps[stage] = System.currentTimeMillis();
            System.err.printf("Stage %d complete at %d (processed: %d)%n", stage, stageEndTimestamps[stage], thisStageCount);
        }

        writeSummaryCsv();
        sc.close();
    }

    private static void writeSummaryCsv() {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/spark_summary.csv")))) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.err.println("✅ Summary written to /tmp/spark_summary.csv");
    }
}
