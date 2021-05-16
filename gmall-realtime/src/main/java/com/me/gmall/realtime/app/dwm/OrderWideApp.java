package com.me.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.me.gmall.realtime.bean.OrderDetail;
import com.me.gmall.realtime.bean.OrderInfo;
import com.me.gmall.realtime.bean.OrderWide;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * Author: Felix
 * Date: 2021/5/15
 * Desc: 订单宽表数据准备
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度读取kafka分区数据
        env.setParallelism(4);
        //TODO 2.设置检查点  略
        //TODO 3.从Kafka中读取数据
        //3.1 声明主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //3.2 获取kafkaSource
        FlinkKafkaConsumer<String> sourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        //3.3 读取数据封装成流
        DataStream<String> orderInfoJsonStrDS = env.addSource(sourceOrderInfo);
        DataStream<String> orderDetailJsonStrDS = env.addSource(sourceOrderDetail);

        //3.4 对订单流中的数据进行结构转换  jsonStr->实体类对象
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(new RichMapFunction<String, OrderInfo>() {
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                //将jison字符串转换为订单实体类
                OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });

        //3.5 对订单明细流中的数据进行结构转换  jsonStr->实体类对象
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(new RichMapFunction<String, OrderDetail>() {
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String jsonStr) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //orderInfoDS.print(">>>>");
        //orderDetailDS.print("#####");

        //TODO 4. 指定Watermark以及提取事件时间字段
        //4.1 订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        }));

        //4.2 订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        //TODO 5. 按照订单id进行分组
        //5.1 订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(OrderInfo::getId);
        //5.2 订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6. 订单和订单明细进行双流join(intervalJoin)
        // 使用orderInfo关联orderDetail
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                // 设置时间上下界
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        // 将关联的key，形成宽表向下游发送
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print(">>>>");
        env.execute();
    }
}
