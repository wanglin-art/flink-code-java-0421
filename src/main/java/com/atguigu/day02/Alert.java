package com.atguigu.day02;

/**
 * 王林
 * 2020/9/27
 **/
public class Alert {
    String message;
    Long timestamp;

    public Alert() {
    }

    public Alert(String message, Long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
