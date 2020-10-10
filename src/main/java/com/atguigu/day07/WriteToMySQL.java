package com.atguigu.day07;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 王林
 * 2020/10/10 09点12分
 **/
public class WriteToMySQL {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream
                .addSink(new MyJDBCSink());

        env.execute();
    }

    public static class MyJDBCSink extends RichSinkFunction<SensorReading>{
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/sensorreading?characterEncoding=utf8&useSSL=false",
                    "root",
                    "root"
            );
            insertStmt = conn.prepareStatement("insert into sensor (id,temp) values (?,?)");
            updateStmt = conn.prepareStatement("update sensor set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1,value.temperature);
            updateStmt.setString(2,value.id);

            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.id);
                insertStmt.setDouble(2,value.temperature);
                insertStmt.execute();
            }

        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
