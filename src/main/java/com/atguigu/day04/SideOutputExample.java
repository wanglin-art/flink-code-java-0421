package com.atguigu.day04;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 王林
 * 2020/9/29 18点54分
 **/
public class SideOutputExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

//        OutputTag<String> outputTag1 = new OutputTag<String>("lessThan32") {
//        };
        OutputTag<String> outputTag2 = new OutputTag<String>("greatThan32") {};
        OutputTag<String> outputTag3 = new OutputTag<String>("greatThan100") {
        };
//        SingleOutputStreamOperator<SensorReading> process1 = stream
//                .process(new ProcessFunction<SensorReading, SensorReading>() {
//                    @Override
//                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
//                        if (value.temperature < 32.0) {
//                            ctx.output(outputTag1, "小于32读的温度值发送到侧输出流中！" + value.temperature);
//                        }
//                        out.collect(value);
//                    }
//                });
        SingleOutputStreamOperator<SensorReading> process2 = stream
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.temperature >87.0) {
                            ctx.output(outputTag2, "id为：" + value.id + "，大于87°的温度值发送到侧输出流中！" + value.temperature);
                        }else if (value.temperature>32.0){
                            ctx.output(outputTag3, "#########id为：" + value.id + "，大于32°的温度值发送到侧输出流中！" + value.temperature);
                        }
                        out.collect(value);
                    }
                });


//        process1.getSideOutput(outputTag1).print();  //调用这个方法只打印侧输出流
//        process2.print();       //只调用这个方法只会打印收集到的数据，在没有执行这个方法的时候，collect只是把数据收集到并没有执行
        process2.getSideOutput(outputTag2).print();
        process2.getSideOutput(outputTag3).print();
        env.execute();
    }
}
