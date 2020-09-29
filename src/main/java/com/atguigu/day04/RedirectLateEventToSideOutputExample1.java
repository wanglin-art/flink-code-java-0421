package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 王林
 * 2020/9/29 19点31分
 **/
public class RedirectLateEventToSideOutputExample1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> operator = stream
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );
        SingleOutputStreamOperator<String> process = operator
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(10))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("last_data") {
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            count += 1;
                        }
                        out.collect("窗口中一共有：" + count + "条数据");
                    }
                });
        process.print();
        process.getSideOutput(new OutputTag<Tuple2<String,Long>>("last_data"){}).print();

        env.execute();
    }
}
