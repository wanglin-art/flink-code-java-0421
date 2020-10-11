package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import sun.rmi.runtime.Log;

import java.util.List;
import java.util.Map;

/**
 * 王林
 * 2020/10/10 13点59分
 **/
public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);





        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user_1", "0.0.0.1", "fail", 1000L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 1500L),
                new LoginEvent("user_1", "0.0.0.3", "fail", 700000L)
//                new LoginEvent("user_2", "0.0.0.4", "fail", 2000L)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        //定义恶意登陆的模板
        Pattern<LoginEvent,LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").iterator().next();
                        LoginEvent second = map.get("second").iterator().next();
                        LoginEvent third = map.get("third").iterator().next();
                        return first.ipAddr + "\n" + second.ipAddr + "\n" + third.ipAddr;
                    }
                })
                .print();

        env.execute();
    }
}
