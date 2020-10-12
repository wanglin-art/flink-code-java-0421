package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 王林
 * 2020/10/12 13点54分
 **/
public class AppendModeExample {
    public static void main(String[] args)  throws Exception{
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //使用流模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance().inStreamingMode().build();

        //创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        //流--》表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime()
        );

        Table tableResult = table.select($("id"));

        tEnv.toAppendStream(tableResult, Row.class).print();


        //sql
        //创建一张临时表
        tEnv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime()
        );

        Table sqlResult = tEnv.sqlQuery("select id from sensor");
        tEnv.toAppendStream(sqlResult,Row.class).print();
        env.execute();
    }
}
