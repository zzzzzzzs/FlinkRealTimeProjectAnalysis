package com.me.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.me.gmall.realtime.bean.OrderWide;
import com.me.gmall.realtime.bean.PaymentInfo;
import com.me.gmall.realtime.bean.PaymentWide;
import com.me.gmall.realtime.utils.DateTimeUtil;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: zs
 * Date: 2021/5/18
 * Desc: 支付宽表准备
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {


        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.设置检查点
        //env.enableCheckpointing(4000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/flink/zk"));
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3.声明消费主题以及消费者组
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //TODO 4.获取Kafka Source
        //4.1 订单宽表
        FlinkKafkaConsumer<String> orderWideKafkaSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        //4.2 支付
        FlinkKafkaConsumer<String> paymentInfoKafkaSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);

        //TODO 5.读取数据封装为流
        //5.1 订单宽表
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideKafkaSource);

        //5.2 支付
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoKafkaSource);

        //TODO 6.对流的数据进行格式转换
        //6.1 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideDS
                = orderWideStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));
        //6.2 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS
                = paymentInfoStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));

        //TODO 7.指定Watermark以及提取事件时间字段
        //7.1 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS =
                orderWideDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                })
                );
        //7.2 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS =
                paymentInfoDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }));
        //TODO 8.KeyBy进行分组  指定连接条件
        //8.1 订单宽表
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS
                .keyBy(orderWide -> orderWide.getOrder_id());


        //8.2 支付
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS
                .keyBy(paymentInfo -> paymentInfo.getOrder_id());


        //TODO 9.双流join
        SingleOutputStreamOperator<PaymentWide> joinedDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                // 下完订单的半个小时内完成支付，orderWide早就到了，所以往前找
                .between(Time.seconds(-1800L), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                })
                .uid("payment_wide_join");


        //TODO 10.打印输出  写回到kafka的主题
        joinedDS.print("paymentWide");

        joinedDS
                .map(paymentWide -> JSON.toJSONString(paymentWide))
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}

