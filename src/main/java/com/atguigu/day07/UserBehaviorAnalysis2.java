package com.atguigu.day07;

import com.atguigu.day06.ItemViewCount;
import com.atguigu.day06.UserBehavior;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 王林
 * 2020/10/11 17点01分
 **/
public class UserBehaviorAnalysis2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("E:\\ideaproject\\flink-code-java-0421\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                })
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
                .aggregate(new MyAgg2(),new MyProcessWFunC())
                .keyBy(r->r.windowEnd)
                .process(new MyKeyedFunC4(3))
                .print();
        env.execute();
    }

    public static class MyAgg2 implements AggregateFunction<UserBehavior,Long,Long>{

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

    public static class MyProcessWFunC extends ProcessWindowFunction<Long, ItemViewCount,String, TimeWindow>{



        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(s,elements.iterator().next(),context.window().getStart(),context.window().getEnd()));
        }
    }

    public static class MyKeyedFunC4 extends KeyedProcessFunction<Long,ItemViewCount,String>{
        private ListState<ItemViewCount> listState;
        private Integer muchNeed;

        public MyKeyedFunC4(int muchNeed) {
            this.muchNeed = muchNeed;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("A",ItemViewCount.class));
        }


        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd+100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ItemViewCount> itemViewCounts = listState.get();
            ArrayList<ItemViewCount> list = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemViewCounts) {
                list.add(itemViewCount);
            }

            list.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer
                    .append("=============================================\n")
                    .append("time:"+new Timestamp(timestamp)+"\n");

            for (Integer i = 0; i < muchNeed; i++) {
                ItemViewCount itemViewCount = list.get(i);
                stringBuffer
                        .append("No."+(i+1))
                        .append("===》"+"itemId:"+itemViewCount.itemId)
                        .append(", count:"+itemViewCount.count)
                        .append("\n");
            }
            stringBuffer
                    .append("=============================================\n\n\n");

            Thread.sleep(2000L);
            out.collect(stringBuffer.toString());
            listState.clear();

        }
    }

}
