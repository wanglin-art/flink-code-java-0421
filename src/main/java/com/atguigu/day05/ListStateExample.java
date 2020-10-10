package com.atguigu.day05;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 王林
 * 2020/9/30 16点12分
 **/
public class ListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ListState<SensorReading> mySensorReading;
                    private ValueState<Long> timer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mySensorReading = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("mySensorReading", SensorReading.class));
                        timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        mySensorReading.add(value);
                        if (timer.value() == null) {
                            long thePrecessTime = ctx.timerService().currentProcessingTime()+ 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(thePrecessTime);
                            timer.update(thePrecessTime);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long   count = 0L;
                        for (SensorReading sensorReading : mySensorReading.get()) {
                            count+=1;
                        }
                        timer.clear();
                        out.collect("10秒时间共接收到了："+count+"条数据");
                    }
                })
                .print();

        env.execute();
    }
}
