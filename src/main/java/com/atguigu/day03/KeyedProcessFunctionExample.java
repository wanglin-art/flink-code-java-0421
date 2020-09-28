package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/28
 **/
public class KeyedProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("hadoop104", 9999);
        stream
               .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
                   @Override
                   public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                       String[] str = value.split(" ");
                       out.collect(Tuple2.of(str[0],str[1]));
                   }
               })
                .keyBy(r -> r.f0)
                .process(new MyKeyedProFunC())
                .print();
        env.execute();
    }

    public static class MyKeyedProFunC extends KeyedProcessFunction<String, Tuple2<String,String>,String>{

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            long processingTime = ctx.timerService().currentProcessingTime()+1000L*10;
            ctx.timerService().registerProcessingTimeTimer(processingTime);
            out.collect("得到的数据是："+value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp,ctx,out);
            out.collect("定时器触发！！！");
        }
    }
}
