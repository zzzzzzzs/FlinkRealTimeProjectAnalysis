package com.me.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.bean.VisitorStats;
import com.me.gmall.realtime.utils.ClickHouseUtil;
import com.me.gmall.realtime.utils.DateTimeUtil;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Author: zs
 * Date: 2021/5/18
 * Desc: 访客主题DWS
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 * 进行关联  这是一个fulljoin
 * 可以考虑使用flinksql 完成
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //TODO 2.设置检查点 略

        //TODO 3.声明消费主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_group";

        //TODO 4.获取KafkaSource
        //4.1 pv
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        //4.2 uv
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        //4.3 ujd
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        //TODO 5.读取数据，封装流
        DataStreamSource<String> pageViewDS = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDS = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDS = env.addSource(userJumpSource);

        //pageViewDS.print(">>>");
        //uniqueVisitDS.print("###");
        //userJumpDS.print("$$$");


        //TODO 6.对读到的流进行结构的转换
        //pv
        SingleOutputStreamOperator<VisitorStats> pageViesStatsDS = pageViewDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    visitorStats.setSv_ct(1L);
                }
                return visitorStats;
            }
        });

        //uv
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uniqueVisitDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });
        //ujd
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        //TODO 7.合并三条流
        DataStream<VisitorStats> unionDS = pageViesStatsDS.union(uvStatsDS, userJumpStatsDS);

//        unionDS.print(">>>:");


        //TODO 8.设置Watermark以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                                return visitorStats.getTs();
                            }
                        }));

        //TODO 9.按照维度进行分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                );
            }
        });

        //TODO 10.开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 11.对窗口中的数据进行聚合计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        //对窗口中的数据进行处理  补充窗口的开始时间和结束时间字段
                        for (VisitorStats visitorStats : elements) {
                            visitorStats.setStt(DateTimeUtil.toYMDHms(new Date(context.window().getStart())));
                            visitorStats.setEdt(DateTimeUtil.toYMDHms(new Date(context.window().getEnd())));
                            // 写到clickhouse，因为中间处理了一下，使用当前时间
                            visitorStats.setTs(System.currentTimeMillis());
                            out.collect(visitorStats);
                        }
                    }
                });
        reduceDS.print(">>>>");
        //TODO 12.将数据写到Clickhouse中
        reduceDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats_1116 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
    }
}
