package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/30 14点01分
 **/
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream<Tuple3<String, Long, String>, String> clickStream = env.fromElements(
                Tuple3.of("user_1", 10 * 60 * 1000L, "click"), //10min
                Tuple3.of("user_1", 16 * 60 * 1000L, "click") //16min
                        //                Tuple3.of("")
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Long, String>, String> browseStream = env.fromElements(
                Tuple3.of("user_1", 5 * 60 * 1000L, "browse"), //5min
                Tuple3.of("user_1", 6 * 60 * 1000L, "browse") //6min
                    //                Tuple3.of("")
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);
        clickStream
                .intervalJoin(browseStream)
                .between(Time.minutes(-10),Time.minutes(0))
                .process(new ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public void processElement(Tuple3<String, Long, String> click, Tuple3<String, Long, String> browse, Context ctx, Collector<String> out) throws Exception {
                        out.collect(click+"==>"+browse);
                    }
                })
                .print();
        env.execute();
    }
}
