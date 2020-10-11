package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 王林
 * 2020/10/10 13点59分
 **/
public class LoginFailDetectOtherMethod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user_1", "0.0.0.1", "fail", 1000L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 1500L),
                new LoginEvent("user_1", "0.0.0.3", "fail", 2000L)
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
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .times(3)
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<LoginEvent> loginEventPatternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);

        loginEventPatternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        List<LoginEvent> first = map.get("first");
                        for (LoginEvent loginEvent : first) {
                            System.out.println(loginEvent.ipAddr);
                        }
                        return "5秒内连续登陆三次！";
                    }
                })
                .print();

        env.execute();
    }
}
