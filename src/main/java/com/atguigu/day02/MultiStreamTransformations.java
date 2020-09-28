package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import com.atguigu.day02.util.SmokeLevelSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/27
 **/
public class MultiStreamTransformations {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SmokeLevel> smokeStream = env.addSource(new SmokeLevelSource()).setParallelism(1);
        DataStreamSource<SensorReading> sensorStream = env.addSource(new SensorSource());

        sensorStream
                .connect(smokeStream.broadcast())
                .flatMap(new MyCoFlatMap())
                .print();

        env.execute();
    }
    public static class  MyCoFlatMap implements CoFlatMapFunction<SensorReading,SmokeLevel,Alert>{
            private     SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
            if (sensorReading.temperature>20 && smokeLevel==SmokeLevel.HIGH){
                collector.collect(new Alert("WARN!!!!"+sensorReading,sensorReading.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
                this.smokeLevel=smokeLevel;
        }
    }
}
