package com.atguigu.day02.util;

/**
 * 王林
 * 2020/9/27
 **/
public class SensorReading {
    public String id ;
    public Long timestamp;
    public Double temperature;

    public SensorReading() {  //必须要有一个空构造器
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
