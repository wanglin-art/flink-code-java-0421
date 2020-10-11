package com.atguigu.day07;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 王林
 * 2020/10/10 13点46分
 **/
public class CheckPointExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.enableCheckpointing(10 * 1000L);

        env.setStateBackend(new FsStateBackend("file:///src/main/resources/checkPoints"));

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream.print();

        env.execute();
    }
}
