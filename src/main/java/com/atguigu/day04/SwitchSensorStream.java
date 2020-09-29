package com.atguigu.day04;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/29 20点24分
 **/
public class SwitchSensorStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<SensorReading, String> stream1 = env
                .addSource(new SensorSource())
                .keyBy(r -> r.id);

        KeyedStream<Tuple2<String, Long>, String> stream2 = env
                .fromElements(Tuple2.of("sensor_1", 10 * 1000L))
                .keyBy(r -> r.f0);

        stream1
                .connect(stream2)
                .process(new MyCoProcessFun())
                .print();

        env.execute();
    }
    public static class MyCoProcessFun extends CoProcessFunction<SensorReading,Tuple2<String,Long>,SensorReading>{
        private ValueState<Boolean>  thisBoolean;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            thisBoolean = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("boolean", Types.BOOLEAN));
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (thisBoolean.value() != null && thisBoolean.value()){
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            thisBoolean.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            thisBoolean.clear();
        }
    }
}
