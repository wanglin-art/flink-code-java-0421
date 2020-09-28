package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 王林
 * 2020/9/28
 **/
public class MinTempWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        SingleOutputStreamOperator<Tuple2<String, Double>> mapStream = stream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return Tuple2.of(value.id, value.temperature);
                    }
                });

        KeyedStream<Tuple2<String, Double>, String> keyByStream = mapStream.keyBy(r -> r.f0);

        WindowedStream<Tuple2<String, Double>, String, TimeWindow> timeDStream = keyByStream.timeWindow(Time.seconds(10),Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Double>> windowsMin = timeDStream.reduce(new ReduceFunction<Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) throws Exception {
                if (v1.f1 > v2.f1) {
                    return v2;
                } else {
                    return v1;
                }
            }
        });

        windowsMin.print();

        env.execute();
    }
}
