package com.me.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Author: zs
 * Date: 2021/5/15
 * Desc: 用户跳出明细统计
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*
        //TODO 2.设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("xxxx"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 3.从kafka中读取数据
        //3.1 声明主题和消费者组
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_app_group";
        String sinkTopic = "dwm_user_jump_detail";
        //3.2 获取kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 读取数据封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

//        DataStream<String> kafkaDS = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );


        //3.4 对流中数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //jsonObjDS.print(">>>>");

        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                //WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        }));

        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 6.配置CEP匹配模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        // 如果lastPageId为空，证明用户访问第一个页面
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .next("second").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        // 如果pageId不为空，则证明用户点击了下一个页面
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                })
                // 事件在10s中内发生
                .within(Time.seconds(10));

        //TODO 7.将模式应用到流中
        PatternStream<JSONObject> filterPS = CEP.pattern(keyedDS, pattern);

        //TODO 8.从流中提取超时数据
        //8.1 定义侧输出流标记（因为超时数据会被放到侧输出流中）
        OutputTag<String> outputTag = new OutputTag<String>("outputTag") {
        };

        //8.2 提取数据
        SingleOutputStreamOperator<String> resDS = filterPS.flatSelect(
                outputTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> firstList = pattern.get("first");
                        //在处理超时数据的时候，写到主流的数据，会打上侧输出流标记
                        for (JSONObject jsonObj : firstList) {
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                },
                //处理未超时数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //注意：因为我们这里要统计的是跳出，所有对未超时数据不需要处理

                    }
                }
        );

        //8.3 获取侧输出流
        DataStream<String> jumpDS = resDS.getSideOutput(outputTag);

        jumpDS.print(">>>>");

        //TODO 9.将跳出明细写到Kafka的dwm层
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
