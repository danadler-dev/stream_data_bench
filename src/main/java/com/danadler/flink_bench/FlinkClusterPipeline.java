package com.danadler.flink_bench;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class FlinkClusterPipeline {
    private static final int RECORD_COUNT = 10_000_000;
    private static final int RECORD_SIZE = 1000;
    private static final int STAGE_COUNT = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Create bounded data stream using Flink's native generator
        DataStream<String> stream = env
                .fromSequence(1, RECORD_COUNT)
                .map(i -> "X".repeat(RECORD_SIZE))
                .name("Generator");

        // Chain the pipeline stages
        for (int i = 0; i < STAGE_COUNT; i++) {
            stream = stream.map(new StageFunction(i)).name("Stage_" + i);
        }

        stream.addSink(new DiscardingSink<>()); // needed to pull all the output

        env.setParallelism(8);
        env.disableOperatorChaining(); // allow parallelism in chain
        env.execute("Flink Cluster 10-Stage Pipeline");
    }

    // Simple map function per stage to simulate processing
    public static class StageFunction implements MapFunction<String, String> {
        private final int stage;
        private long latestTime = 0;
        private int seen = 0;

        public StageFunction(int stage) {
            this.stage = stage;
        }

        @Override
        public String map(String value) throws Exception {
            seen++;
            return String.valueOf((char) ('0' + stage)).repeat(value.length());
        }
    }
}
