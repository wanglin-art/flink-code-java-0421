package com.atguigu.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 王林
 * 2020/9/27
 **/
public class SensorSourceTest extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {

        String[] sensorIds = new String[10];
        double[] temp = new double[10];

        Random rand = new Random();
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_"+(i+1);
            temp[i] = 65 + rand.nextGaussian()+20;
        }
        while (running){
            Long curT= Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                temp[i] += rand.nextGaussian()*0.5;
                sourceContext.collect(new SensorReading(sensorIds[i],curT,temp[i]));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running= false;
    }
}
