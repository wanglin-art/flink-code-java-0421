package com.atguigu.day07;

/**
 * 王林
 * 2020/10/10 13点56分
 **/
public class LoginEvent {
    public String userId;
    public String ipAddr;
    public String eventType;
    public Long eventTime;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddr, String eventType, Long eventTime) {
        this.userId = userId;
        this.ipAddr = ipAddr;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddr='" + ipAddr + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
