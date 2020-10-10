package com.atguigu.day06;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

/**
 * 王林
 * 2020/10/9 15点37分
 **/
public class UserBehaviorProduceToKafka {
    public static void main(String[] args) throws Exception{
                writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception{
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        File file = new File("E:\\ideaproject\\flink-code-java-0421\\src\\main\\resources\\UserBehavior.csv");
        Scanner sc = new Scanner(file);
        while (sc.hasNextLine()){
           producer.send(new ProducerRecord<String,String>(topic,sc.nextLine()));
        }
    }
}
