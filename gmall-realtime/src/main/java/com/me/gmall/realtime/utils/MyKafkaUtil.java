package com.me.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static com.me.gmall.realtime.common.KafkaConfig.KAFKA_SERVER;

/**
 * Author: zs
 * Date: 2021/5/11
 * Desc: 操作Kafka的工具类
 */
public class MyKafkaUtil {
    //获取flink封装的kafka消费者对象
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                topic,new SimpleStringSchema(),props
        );
        return kafkaSource;
    }

    //获取flink封装的kafka生产者对象
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }
}
