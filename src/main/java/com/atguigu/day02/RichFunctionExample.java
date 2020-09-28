package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 王林
 * 2020/9/27
 **/
public class RichFunctionExample{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
        stream
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("开始声明周期");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value+1;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期结束");
                    }
                })
                .print();

        env.execute();
    }
}
