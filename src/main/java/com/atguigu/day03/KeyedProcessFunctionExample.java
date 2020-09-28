package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * 王林
 * 2020/9/28
 **/
public class KeyedProcessFunctionExample {

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//aaa
//        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
//        streams
//                .keyBy(r->r.id)
//                .process(new MyKeyedProFunC())
//                .print();
//        env.execute();
//    }
//    public static class MyKeyedProFunC extends KeyedProcessFunction<String,>
}
