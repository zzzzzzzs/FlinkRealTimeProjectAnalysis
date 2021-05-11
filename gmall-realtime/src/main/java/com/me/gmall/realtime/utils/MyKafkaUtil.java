package com.me.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Author: zs
 * Date: 2021/5/11
 * Desc: 操作Kafka的工具类
 */
public class MyKafkaUtil {

    //获取flink封装的kafka消费者对象
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"aliyun102:9092,aliyun103:9092,aliyun104:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                topic,new SimpleStringSchema(),props
        );
        return kafkaSource;
    }
}
