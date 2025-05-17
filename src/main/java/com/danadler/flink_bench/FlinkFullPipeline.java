package com.danadler.flink_bench;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Full pipeline benchmarking for Flink.
 * All within a single process. Does not require flink docker to run.
 */
public class FlinkFullPipeline {
    private static final int STAGE_COUNT = 10;
    private static final int RECORD_COUNT = 100_000_000;
    private static final int RECORD_SIZE = 1000;
    private static final long[] stageEndTimestamps = new long[STAGE_COUNT];
    private static final long startTime = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // start single-threaded for benchmarking

        // Generate source data without materializing it all to avoid out of memory
        DataStream<String> stream = env.addSource(new PayloadSource(RECORD_COUNT, RECORD_SIZE));

        // Apply 10 sequential stages
        for (int i = 0; i < STAGE_COUNT; i++) {
            stream = stream.map(new StageMapFunction(i)).name("Stage_" + i);
        }

        stream.process(new ProcessFunction<String, String>() {
                    private int seen = 0;

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        if (seen++ < 10) {
                            out.collect("Sample: " + value.substring(0, 20) + " ..." + value.length());
                        }
                    }
                })
                .print();

        env.execute("Flink 10-Stage In-Memory Pipeline");
        stageEndTimestamps[0] = startTime;

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("flink_summary.csv")))) {
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
            System.err.println("✅ Flink benchmark summary written to flink_summary.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class StageMapFunction implements MapFunction<String, String> {
        private final int stage;
        private final AtomicInteger counter = new AtomicInteger();

        public StageMapFunction(int stage) {
            this.stage = stage;
        }

        @Override
        public String map(String value) {
            int processed = counter.incrementAndGet();
            if (processed == RECORD_COUNT) {
                stageEndTimestamps[stage] = System.currentTimeMillis();
            }
            return String.valueOf((char) ('0' + stage)).repeat(value.length());
        }
    }

    public static class PayloadSource implements SourceFunction<String> {
        private final int recordCount;
        private final int recordSize;

        public PayloadSource(int recordCount, int recordSize) {
            this.recordCount = recordCount;
            this.recordSize = recordSize;
        }

        @Override
        public void run(SourceContext<String> ctx) {
            char[] chars = new char[recordSize];
            Arrays.fill(chars, 'X');
            String payload = new String(chars);

            for (int i = 0; i < recordCount; i++) {
                ctx.collect(payload);
            }
        }

        @Override
        public void cancel() {}
    }
}
