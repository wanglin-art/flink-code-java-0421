package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 王林
 * 2020/9/28
 **/
public class AvgTempWithWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .process(new MyProcessWindow())
                .print();
        env.execute();
    }
    public static class MyProcessWindow extends ProcessWindowFunction<SensorReading,String,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<SensorReading> iterable, Collector<String> collector) throws Exception {
            Double sum = 0.0;
            long count = 0L;

            for (SensorReading sensor : iterable) {
                sum+=sensor.temperature;
                count+=1L;
            }
            collector.collect("传感器为："+key+" 窗口的结束时间是："+new Timestamp(context.window().getEnd()) +"的平均值是："+sum/count);
        }
    }
}
