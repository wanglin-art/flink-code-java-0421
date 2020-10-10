package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;


/**
 * 王林
 * 2020/10/9 11点46分
 **/
public class UserBehaviorAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("E:\\ideaproject\\flink-code-java-0421\\src\\main\\resources\\UserBehavior.csv")
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
                );
        stream
                .keyBy(r->r.itemId)
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new MyAgg(),new MyProWindF())
                .keyBy(r->r.windowEnd)
                .process(new MyKeyedFunC2(3))
                .print();



        env.execute();
    }

    public static class MyAgg implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
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

    public static class MyProWindF extends ProcessWindowFunction<Long,ItemViewCount,String, TimeWindow>{


        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(key,elements.iterator().next(),context.window().getStart(),context.window().getEnd()));
        }
    }

    public static class MyKeyedFunC2 extends KeyedProcessFunction<Long,ItemViewCount,String>{

        private ListState<ItemViewCount> itemViewCountListState;
        private Integer threshold;

        public MyKeyedFunC2(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemViewCountListState= getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("thisAnList",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> myList = new ArrayList<>();
            Iterable<ItemViewCount> itemViewCounts = itemViewCountListState.get();
            for (ItemViewCount itemViewCount : itemViewCounts) {
                myList.add(itemViewCount);
            }
            itemViewCountListState.clear();

            myList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            StringBuffer myStrBuff = new StringBuffer();

            myStrBuff
                    .append("=================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");

            for (int i = 0; i < this.threshold; i++) {
                ItemViewCount currItem = myList.get(i);
                myStrBuff
                        .append("No.")
                        .append(i+1)
                        .append(":")
                        .append(currItem.itemId)
                        .append("count=")
                        .append(currItem.count)
                        .append("\n");
            }


                    myStrBuff.append("=================================\n");
            Thread.sleep(1000L);
            out.collect(myStrBuff.toString());
        }
    }

}
