package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 王林
 * 2020/9/29 19点49分
 **/
public class RedirectLateEventToSideOutputExample2 {
    public static void main(String[] args) throws Exception {
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
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            ctx.output(new OutputTag<String>("last-data") {
                            }, "时间戳为：" + value.f1 + "的数据迟到了！");
                        } else {
                            out.collect("时间戳为：" + value.f1 + "的数据没有迟到！");
                        }
                    }
                });

        process.print();
        process.getSideOutput(new OutputTag<String>("last-data") {}).print();

        env.execute();
    }
}
