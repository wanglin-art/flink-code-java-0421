package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

/**
 * 王林
 * 2020/10/10 16点25分
 **/
public class LoginFailDetectWithoutCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
//                new LoginEvent("user_1", "0.0.0.4", "success", 3500L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 4000L)
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

        stream
                .keyBy(r -> r.userId)
                .process(new MyProcessFunC2())
                .print();

        env.execute();
    }

    public static class MyProcessFunC2 extends KeyedProcessFunction<String, LoginEvent, String> {
        private ListState<LoginEvent> listState;
        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("listState", LoginEvent.class));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Types.LONG));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            if (value.eventType.equals("success")) {
                listState.clear();
                if (valueState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(value.eventTime + 5000L);
                    valueState.clear();
                }

            } else {
                listState.add(value);
                if (valueState.value() == null) {
                    ctx.timerService().registerEventTimeTimer(value.eventTime + 5000L);
                    valueState.update(value.eventTime + 5000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            if (listState.get() != null) {
                long count = 0L;

                Iterable<LoginEvent> loginEvents = listState.get();
                for (LoginEvent loginEvent : loginEvents) {
                    count += 1;
                }
                if (count > 2) {
                    out.collect("恶意登陆检测！");

                }
            }

            listState.clear();
            valueState.clear();

        }

    }
}
