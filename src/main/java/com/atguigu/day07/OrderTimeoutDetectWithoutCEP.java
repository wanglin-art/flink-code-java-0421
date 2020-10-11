package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/10/11 10点07分
 **/
public class OrderTimeoutDetectWithoutCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        SingleOutputStreamOperator<OrderEvent> stream = env
                .fromElements(
                        new OrderEvent("order_1", "create", 1000L),
                        new OrderEvent("order_2", "create", 2000L),
                        new OrderEvent("order_2", "pay", 3000L)
//                        new OrderEvent("order_1", "pay", 11000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );
        stream
                .keyBy(r -> r.orderId)
                .process(new MyKeyedFunC3())
                .print();


        env.execute();
    }

    public static class MyKeyedFunC3 extends KeyedProcessFunction<String, OrderEvent, String> {
        private ValueState<OrderEvent> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("list", OrderEvent.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if (value.eventType.equals("create")) {

                if (listState.value() != null) {
                    out.collect("订单ID为:" + ctx.getCurrentKey() + "的订单支付成功！");
                } else {
                    listState.update(value);
                    ctx.timerService().registerEventTimeTimer(value.eventTime+10000L);
                }
            } else {
                listState.update(value);
                out.collect("订单ID为:" + ctx.getCurrentKey() + "的订单支付成功！");
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if(listState.value()!=null && listState.value().eventType.equals("create")){
                out.collect("订单ID为："+ctx.getCurrentKey()+"的订单没有支付成功！");
            }

        }
    }
}
