package com.me.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.app.func.DimAsyncFunction;
import com.me.gmall.realtime.bean.OrderDetail;
import com.me.gmall.realtime.bean.OrderInfo;
import com.me.gmall.realtime.bean.OrderWide;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Author: zs
 * Date: 2021/5/15
 * Desc: 订单宽表数据准备---：需要从用户表中获取用户的信息做关联
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

//        orderWideDS.print(">>>>");

        //TODO 7.和用户维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                // 使用了模板方法设计模式
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJsonObj) {
                        try {
                            // 将 OrderWide 的字段补全
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            String gender = dimJsonObj.getString("GENDER");
                            String birthday = dimJsonObj.getString("BIRTHDAY");
                            Date birthdayDate = sdf.parse(birthday);
                            long birthdayTime = birthdayDate.getTime();
                            long curTime = System.currentTimeMillis();
                            long ageTime = curTime - birthdayTime;
                            Long age = ageTime / 1000 / 60 / 60 / 24 / 365;
                            orderWide.setUser_gender(gender);
                            orderWide.setUser_age(age.intValue());
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }

                    // 获取key
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithUserInfoDS.print(">>>>");
        //TODO 8.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserInfoDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJsonObj) {
                        orderWide.setProvince_name(dimJsonObj.getString("NAME"));
                        orderWide.setProvince_3166_2_code(dimJsonObj.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(dimJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(dimJsonObj.getString("AREA_CODE"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //orderWideWithProvinceDS.print(">>>>");

        //TODO 9.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 10.关联spu
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 11.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);


        //TODO 12.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithTmDS.print(">>>>");

        //TODO 13.将数据写回到kafka的dwm层
        orderWideWithTmDS
                .map(orderWide -> JSON.toJSONString(orderWide))
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        env.execute();
    }
}
