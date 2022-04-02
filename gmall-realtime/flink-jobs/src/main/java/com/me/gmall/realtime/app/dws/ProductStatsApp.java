package com.me.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.app.func.DimAsyncFunction;
import com.me.gmall.realtime.common.GmallConstant;
import com.me.gmall.realtime.bean.OrderWide;
import com.me.gmall.realtime.bean.PaymentWide;
import com.me.gmall.realtime.bean.ProductStats;
import com.me.gmall.realtime.utils.ClickHouseUtil;
import com.me.gmall.realtime.utils.DateTimeUtil;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: zs
 * Date: 2021/5/21
 * Desc: 商品统计应用类
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*
        //TODO 2.设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setStateBackend(new FsStateBackend("hdfs://xxx"));
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 3.声明消费主题以及消费者组
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //TODO 4.获取KafkaSource
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        //TODO 5.读取数据封装流
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 6.对读取到的流的数据进行结构的转换
        //6.1 页面日志流---获取点击数以及曝光数
        SingleOutputStreamOperator<ProductStats> pageInfoStatsDS = pageViewDStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        //将json字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取页面的json对象
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        //获取页面id
                        String pageId = pageJsonObj.getString("page_id");
                        //获取事件时间
                        Long ts = jsonObj.getLong("ts");
                        //判断是否为商品详情页  如果是商品详情页的话，那么说明是一个点击行为
                        if ("good_detail".equals(pageId)) {
                            //封装商品统计对象，并且记录点击行为
                            //获取商品的id
                            Long skuId = pageJsonObj.getLong("item");
                            // TODO 使用了lombok的构造者设计模式
                            ProductStats productStats = ProductStats
                                    .builder()
                                    .sku_id(skuId)
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);
                        }
                        //判断是否为曝光
                        JSONArray displaysArr = jsonObj.getJSONArray("displays");
                        if (displaysArr != null && displaysArr.size() > 0) {
                            for (int i = 0; i < displaysArr.size(); i++) {
                                //获取一个曝光对象
                                JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                                //获取曝光对象的类型
                                String itemType = displayJsonObj.getString("item_type");
                                if ("sku_id".equals(itemType)) {
                                    //如果曝光的是商品   那将曝光的商品封装为商品统计对象
                                    Long skuId = displayJsonObj.getLong("item");
                                    ProductStats productStats = ProductStats
                                            .builder()
                                            .sku_id(skuId)
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );
        //6.2 处理收藏行为数据
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        ProductStats productStats = ProductStats
                                .builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(DateTimeUtil.toTs(jsonObj.getString("create_time")))
                                .build();
                        return productStats;
                    }
                }
        );
        //6.3 加购行为处理
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject cartJsonObj = JSON.parseObject(jsonStr);
                        ProductStats productStats = ProductStats
                                .builder()
                                .sku_id(cartJsonObj.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(DateTimeUtil.toTs(cartJsonObj.getString("create_time")))
                                .build();
                        return productStats;
                    }
                }
        );
        //6.4  下单行为处理
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        ProductStats productStats = ProductStats
                                .builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                // 感觉这里使用Hashset没用，主要是为了使用注解+反射 判断TransientSink，不将数据写入到clickhouse
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                                .build();
                        return productStats;
                    }
                }
        );

        //6.5  支付行为处理
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats
                            .builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });

        //6.6  退单行为处理
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;

                });

        //6.7  评论行为处理
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDStream.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                });

        //TODO 7.使用union合并为一条流
        DataStream<ProductStats> unionDS = pageInfoStatsDS.union(
                favorInfoStatsDS,
                cartInfoStatsDS,
                orderWideStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );

        //unionDS.print(">>>>");

        //TODO 8.指定Watermark并且提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                        return productStats.getTs();
                                    }
                                }
                        ));

        //TODO 9.使用keyby进行分组   因为所有流都有 sku_id，所以就用 sku_id 分组了
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id);

        //TODO 10.开窗   滚动事件时间窗口 10s
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        //TODO 11.对窗口中的元素进行聚合计算  补充统计时间
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        for (ProductStats productStats : elements) {
                            productStats.setStt(DateTimeUtil.toYMDHms(new Date(context.window().getStart())));
                            productStats.setEdt(DateTimeUtil.toYMDHms(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }
                    }
                }
        );
        //TODO 12.补充维度信息
        //关联sku维度
        SingleOutputStreamOperator productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                        productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                        productStats.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 13. 打印  写到ClickHouse中
        productStatsWithTmDstream.print(">>>>");

        productStatsWithTmDstream.addSink(
                ClickHouseUtil.getJdbcSink("insert into product_stats_1116 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
