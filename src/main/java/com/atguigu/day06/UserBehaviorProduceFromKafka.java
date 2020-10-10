package com.atguigu.day06;

import kafka.server.DynamicConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * 王林
 * 2020/10/9 15点45分
 **/
public class UserBehaviorProduceFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> hotitems = env
                .addSource(new FlinkKafkaConsumer011<String>(
                        "hotitems",
                        new SimpleStringSchema(),
                        properties
                ));

        hotitems
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new MyAgg2(), new MyProcessFunC())
                .keyBy(r -> r.windowEnd)
                .process(new MyKeyedFunC2(3))
                .print();

        env.execute();
    }

    public static class MyAgg2 implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class MyProcessFunC extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class MyKeyedFunC2 extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private ListState<ItemViewCount> listState;

        private Integer threshold;      //threshold:  入口；门槛；开始；极限；临界值

        public MyKeyedFunC2(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("list-stats", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ItemViewCount> itemViewCounts = listState.get();
            ArrayList<ItemViewCount> list = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemViewCounts) {
                list.add(itemViewCount);
            }
            listState.clear();

            list.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });


            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer
                    .append("=============================================")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");
            for (int i = 0; i < threshold; i++) {
                ItemViewCount itemViewCount = list.get(i);
                stringBuffer
                        .append("No.")
                        .append(i+1)
                        .append(" : ")
                        .append(itemViewCount.itemId)
                        .append(" count = ")
                        .append(itemViewCount.count)
                        .append("\n");
            }
            stringBuffer.append("=============================================\n\n\n");
            Thread.sleep(1000L);
            out.collect(stringBuffer.toString());
        }
    }

}
