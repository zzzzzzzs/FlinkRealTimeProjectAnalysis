package com.me.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * Author: Felix
 * Date: 2021/5/14
 * Desc: 独立访客明细
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.设置检查点 略
        //TODO 3.从Kafka中读取数据
        //3.1 声明消费者主题以及消费者组
        String topic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        //3.2 获取kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 获取数据封装流
        DataStreamSource<String> kafkaDS = (DataStreamSource<String>) env.addSource(kafkaSource);
        //3.4 对流中数据结构进行转换  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //jsonObjDS.print(">>>>");

        //TODO 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.对当前设备的访问情况进行过滤  UV
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(
                new RichFilterFunction<JSONObject>() {
                    private ValueState<String> lastVisitDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //创建状态描述器
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //状态存活时间配置
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1L))
                                //设置状态修改后，失效时间是否会被修改 默认值
                                //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //设置状态过期后，如果获取状态是否返回  默认值
                                //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        //设置状态存活时间
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    // TODO 使用状态进行去重
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //获取上级页面id
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        //判断是否有上级访问页面  如果有上级访问页面，直接将该日志过滤掉(false过滤)
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }
                        //获取状态的值 --上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取页面日志当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curDate = sdf.format(ts);
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(curDate)) {
                            return false;
                        } else {
                            lastVisitDateState.update(curDate);
                            return true;
                        }
                    }
                }
        );

        filterDS.print(">>>");

        //将UV写回到Kafka的dwm层
        filterDS
                // 将jsonObj转成jsonStr
                .map(jsonObj->jsonObj.toJSONString())
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
