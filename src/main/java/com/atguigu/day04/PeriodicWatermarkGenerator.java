package com.atguigu.day04;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;

/**
 * 王林
 * 2020/9/29 17点49分
 **/
public class PeriodicWatermarkGenerator {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(r-> Tuple2.of(r.split(" ")[0],Long.parseLong(r.split(" ")[1])*1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                  long bound = 5000L;
                  long maxTs = Long.MIN_VALUE+bound+1;
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTs-bound-1);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        maxTs = Math.max(element.f1, maxTs);
                        return element.f1;
                    }
                })
                .keyBy(r->r.f0)
                .process(new MyProFun())
                .print();

        env.execute();
    }
    public static class MyProFun extends KeyedProcessFunction<String,Tuple2<String,Long>,String>{

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            ctx.timerService().registerEventTimeTimer(value.f1+10*1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("时间戳为："+new Timestamp(timestamp)+"的定时触发器触发了！");
        }
    }
}
