package com.me.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

import static com.me.gmall.realtime.common.KafkaConfig.*;

/**
 * @author: zs
 * @Description kafka测试
 * @Date 2021/9/17
 **/
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组

        //3.2 通过工具类  获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("test", "test-group");

        //3.3 将消费到的数据封装到流中
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        kafkaDS.print();

        env.execute();
    }
}
