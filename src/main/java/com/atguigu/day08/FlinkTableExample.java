package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 王林
 * 2020/10/12 09点50分
 **/
public class FlinkTableExample {
    public static void main(String[] args) throws Exception {
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        //创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //流---(放入)--->表
        Table table = tEnv
                .fromDataStream(
                        stream,
                        $("id"),
                        $("timestamp").as("ts"),
                        $("temperature"),
                        $("pt").proctime()
                );
        //table API
        Table tableResult = table
                //10s的滚动窗口，使用pt字段，也就是处理时间，给窗口取一个别名win
                .window(Tumble.over(lit(10).seconds()).on($("pt")).as("win"))
                //。keyBy(id).timeWindow(10s)
                .groupBy($("id"), $("win"))
                .select($("id"), $("id").count());
//        tEnv.toRetractStream(tableResult, Row.class).print();

        //sql
        //创建一张临时表
        tEnv
                .createTemporaryView(
                        "sensor",
                        stream,
                        $("id"),
                        $("timestamp").as("ts"),
                        $("temperature"),
                        $("pt").proctime()
                );
        Table sqlResult = tEnv
                .sqlQuery("select id , count(id) from sensor group By id , Tumble(pt,interval '10' second)");

        tEnv.toRetractStream(sqlResult, Row.class).print();
        env.execute();
    }
}
