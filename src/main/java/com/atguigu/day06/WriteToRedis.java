package com.atguigu.day06;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 王林
 * 2020/10/9 18点23分
 **/
public class WriteToRedis {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
        source.addSink(new RedisSink<SensorReading>(conf,new MyRedisSink()));

        env.execute();
    }
    public static class MyRedisSink implements RedisMapper<SensorReading>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.id;
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.temperature + "";
        }
    }
}
