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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/30 14点58分
 **/
public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r->r.f0)
                .timeWindow(Time.seconds(5))
                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            count+=1;
                        }
                        out.collect("窗口中共有："+count+"个元素");
                    }
                })
                .print();
        env.execute();
    }
    public static class MyTrigger extends Trigger<Tuple2<String,Long>,TimeWindow>{

        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isUpdate = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN));

            if(isUpdate.value() == null){
                long ts = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                ctx.registerEventTimeTimer(ts);
                ctx.registerEventTimeTimer(window.getEnd());
            }
            return TriggerResult.CONTINUE;

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
           if(time == window.getEnd()){
               return TriggerResult.FIRE_AND_PURGE;
           }else{
               long ts = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
               if(ts < window.getEnd()){
                   ctx.registerEventTimeTimer(ts);
               }

               return TriggerResult.FIRE;

           }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> isUpdate =ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("isUpdate",Types.BOOLEAN));
        isUpdate.clear();
        }
    }
}
