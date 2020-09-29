package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 王林
 * 2020/9/29 11点31分
 **/
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] s = value.split(" ");

                        return Tuple2.of(s[0],Long.parseLong(s[1])*1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r->r.f0)
                .process(new Keyed())
                .print();
        env.execute();
    }
    public static class Keyed extends KeyedProcessFunction<String,Tuple2<String,Long>,String>{

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            out.collect("当前的水位线是："+ctx.timerService().currentWatermark()+"，key是："+value.f0);
        }
    }
}
