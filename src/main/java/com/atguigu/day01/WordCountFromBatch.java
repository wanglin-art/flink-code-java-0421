package com.atguigu.day01;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author 王林
 **/
public class WordCountFromBatch {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stream = env.fromElements("hello world", "hello hello world");
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] arr = s.split(" ");
                        for (String word : arr) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1);
        result.print();
        env.execute();

    }
}
