package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 王林
 * 2020/10/10 15点01分
 **/
public class OrderTimeoutDetect {
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

        Pattern<OrderEvent, OrderEvent> orderEventOrderEventPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<OrderEvent> pattern = CEP.pattern(stream.keyBy(r -> r.orderId), orderEventOrderEventPattern);

        SingleOutputStreamOperator<String> select = pattern
                .select(
                        new OutputTag<String>("order_timeOut") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                                return "订单ID为：" + map.get("create").get(0).orderId + "没有支付";
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                                return "订单ID为：" + map.get("pay").get(0).orderId + "已经支付";
                            }
                        }
                );

        select.print();
        select.getSideOutput(new OutputTag<String>("order_timeOut") {
        }).print();


        env.execute();
    }
}
