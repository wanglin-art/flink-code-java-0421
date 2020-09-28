package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import com.atguigu.day03.util.HighLowTemp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/28
 **/
public class HighLowTepPerWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAgg(),new MyProcessW())
                .print();

        env.execute();
    }
    //这是一个注释
    public static class MyAgg implements AggregateFunction<SensorReading, Tuple3<String,Double,Double>,Tuple3<String,Double,Double>>{

        @Override
        public Tuple3<String, Double, Double> createAccumulator() {
            return Tuple3.of("",Double.MIN_VALUE,Double.MAX_VALUE);
        }

        @Override
        public Tuple3<String, Double, Double> add(SensorReading value, Tuple3<String, Double, Double> accumulator) {
            return Tuple3.of(value.id,Math.max(value.temperature,accumulator.f1),Math.min(value.temperature,accumulator.f2));
        }

        @Override
        public Tuple3<String, Double, Double> getResult(Tuple3<String, Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, Double, Double> merge(Tuple3<String, Double, Double> a, Tuple3<String, Double, Double> b) {
            return null;
        }
    }

    public static class MyProcessW extends ProcessWindowFunction<Tuple3<String,Double,Double>, HighLowTemp,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Double, Double>> elements, Collector<HighLowTemp> out) throws Exception {
            Tuple3<String, Double, Double> thisOne = elements.iterator().next();

            out.collect(new HighLowTemp(key,thisOne.f1,thisOne.f2,context.window().getStart(),context.window().getEnd()));
        }
    }
}
