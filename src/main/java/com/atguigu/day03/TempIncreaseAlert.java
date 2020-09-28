package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/28
 **/
public class TempIncreaseAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .keyBy(r->r.id)
                .process(new MyTempAlertProFun())
                .print();
        env.execute();
    }
    public static class MyTempAlertProFun extends KeyedProcessFunction<String,SensorReading,String>{
        private ValueState<Double> lastTemp;
        private ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp=getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            currentTimer =getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer",Types.LONG));
        }

        @Override
        public void processElement(SensorReading r, Context ctx, Collector<String> out) throws Exception {
            double preTemp = 0.0;
            if (lastTemp.value()!=null){
            preTemp = lastTemp.value();
            }
            lastTemp.update(r.temperature);

            long timer = 0L;
            if(currentTimer.value()!=null){
                timer= currentTimer.value();
            }

            if (preTemp==0.0 || r.temperature<preTemp){
                ctx.timerService().deleteProcessingTimeTimer(timer);
                currentTimer.clear();
            }else if(r.temperature > preTemp && timer==0L){
                long newTimer = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(newTimer);
                currentTimer.update(newTimer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("温度控制器ID为："+ctx.getCurrentKey()+",温度连续超过1S上升");
            currentTimer.clear();
        }
    }
}
