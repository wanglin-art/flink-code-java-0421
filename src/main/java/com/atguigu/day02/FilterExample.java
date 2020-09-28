package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 王林
 * 2020/9/27
 **/
public class FilterExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        env.setParallelism(1);
        stream
                .filter(t->t.id.equals("sensor_1"));
//                .print();
        stream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading) throws Exception {
                        return sensorReading.id.equals("sensor_1");
                    }
                });
//                .print();
        stream
                .filter(new MyFilter())
                .print();

        env.execute();
    }
    public static class MyFilter implements FilterFunction<SensorReading>{

        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.id.equals("sensor_1");
        }
    }

}
