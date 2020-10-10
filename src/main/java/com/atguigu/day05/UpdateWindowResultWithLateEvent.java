package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 王林
 * 2020/9/30 09点30分
 **/
public class UpdateWindowResultWithLateEvent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] s = value.split(" ");
                        return Tuple2.of(s[0], Long.parseLong(s[1]) * 1000L);
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
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new MyProcessWindow2())
                .print();


        env.execute();
    }
    public static class MyProcessWindow2   extends ProcessWindowFunction<Tuple2<String,Long>,String,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            long count = 0L;
            for (Tuple2<String, Long> element : elements) {
                count+=1;
            }

            ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN));


            if(isUpdate.value() == null ){
                out.collect("第一次计算窗口，窗口中一共有："+count+"个元素");
                isUpdate.update(true);
            }else{
                out.collect("窗口更新了！,共有："+count+"条数据");
            }
        }
    }
}
