package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/27
 **/
public class FlatMapExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.fromElements("white", "black", "gray");
        stream
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if(value.equals("white")){
                            out.collect(value);
                        }else if(value.equals("black")){
                            out.collect(value);
                            out.collect(value);
                        }
                    }
                });
//                .print();
        stream
                .flatMap(new MyFlatMap())
                .print();
        env.execute();
    }
    public static class MyFlatMap implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("white")){
                out.collect(value);
            }else if(value.equals("black")){
                out.collect(value);
                out.collect(value);
            }
        }
    }
}
