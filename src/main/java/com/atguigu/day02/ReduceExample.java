package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 王林
 * 2020/9/27
 **/
public class ReduceExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.id.equals("sensor_1");
                    }
                })
                .map(new MapFunction<SensorReading, Tuple2<String,Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return Tuple2.of(value.id,value.temperature);
                    }
                })
                .keyBy(t->t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) throws Exception {
                        if (t1.f1>t2.f1){
                            return t1;
                        }else{
                            return t2;
                        }
                    }
                })
                .print();
        env.execute();
    }
}
