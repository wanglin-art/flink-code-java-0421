package com.atguigu.day02.util;

import com.atguigu.day02.SmokeLevel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 王林
 * 2020/9/27
 **/
public class SmokeLevelSource extends RichParallelSourceFunction<SmokeLevel> {
    public boolean running =true;
    @Override
    public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
        Random rand = new Random();
        while (running){
            if(rand.nextGaussian()>0.8){
                sourceContext.collect(SmokeLevel.HIGH);
            }else{
                sourceContext.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
