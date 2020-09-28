package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 王林
 * 2020/9/28
 **/
public class AggregateFunctionExample {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAggregate())
                .print();

        env.execute();
    }
    public static class MyAggregate implements AggregateFunction<SensorReading, Tuple2<String,Double>,Tuple2<String,Double>>{

        @Override
        public Tuple2<String, Double> createAccumulator() {
            return Tuple2.of(" ",Double.MAX_VALUE);
        }

        @Override
        public Tuple2<String, Double> add(SensorReading value, Tuple2<String, Double> acc) {
            if(value.temperature>acc.f1){
                return acc;
            }else{
                return Tuple2.of(value.id,value.temperature);
            }
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> a, Tuple2<String, Double> b) {
            return null;
        }
    }
}
