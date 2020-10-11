package com.atguigu.day07;

/**
 * 王林
 * 2020/10/10 14点59分
 **/
public class OrderEvent {
    public String orderId;
    public String eventType;
    public Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
