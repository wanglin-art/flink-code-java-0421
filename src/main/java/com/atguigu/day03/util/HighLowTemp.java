package com.atguigu.day03.util;

import java.sql.Timestamp;

/**
 * 王林
 * 2020/9/28
 **/
public class HighLowTemp {
    public String id;
    public Double high,low;
    public Long startWindow,endWindow;

    public HighLowTemp() {
    }

    public HighLowTemp(String id, Double high, Double low, Long startWindow, Long endWindow) {
        this.id = id;
        this.high = high;
        this.low = low;
        this.startWindow = startWindow;
        this.endWindow = endWindow;
    }

    @Override
    public String toString() {
        return "HighLowTemp{" +
                "id='" + id + '\'' +
                ", high=" + high +
                ", low=" + low +
                ", startWindow=" + new Timestamp(startWindow) +
                ", endWindow=" + new Timestamp(endWindow) +
                '}';
    }
}
