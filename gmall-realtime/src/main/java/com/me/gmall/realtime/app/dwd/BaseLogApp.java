package com.me.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * Author: zs
 * Date: 2021/5/11
 * Desc: 日志数据的分流
 * Flink的状态
 * -算子状态
 * -键控状态
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.设置检查点
        //2.1 每隔5秒创建一次检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置取消job后是否保留检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置检查点重启策略，如果重启失败，需要自己看日志检查错误
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //2.5 设置状态后端   内存|文件系统|RocksDB
        // TODO 这里在HDFS路径中的chk-3是checkpoint的文件，会5秒变一次内容
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmallFlinkRealTime/ck"));
        //2.6 设置hadoop的用户，否则用户名是电脑的用户，没有权限
        System.setProperty("HADOOP_USER_NAME","atguigu");


        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";

        //3.2 通过工具类  获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 将消费到的数据封装到流中
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.4 对流中数据结构进行转换  jsonStr->JSONObject

        //3.4.1 匿名内部类
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
//            new MapFunction<String, JSONObject>() {
//                @Override
//                public JSONObject map(String jsonStr) throws Exception {
//                    return JSON.parseObject(jsonStr);
//                }
//            }
//        );
        //3.4.2 lambda表达式
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(jsonStr -> JSON.parseObject(jsonStr));

        //3.4.3 方法默认调用
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //jsonObjDS.print(">>>>>");

        //TODO 4.新老访客状态修复
        //思路：将设备上次访问时间记录到状态中，下次再访问的时候，从状态中获取时间，如果有说明访问过，修复；
        // 如果没有，说明没有访问过，将这次访问的时间记录到状态中

        //4.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //4.2 状态修复
        SingleOutputStreamOperator<JSONObject> jsonObjWithFlagDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //注意：不能在声明的时候直接初始化，这个时候获取不到getRuntimeContext
                    private ValueState<String> lastVisitDateState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        lastVisitDateState =
                                getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDateState", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取状态标记
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //获取当前访问日期
                        String curDate = sdf.format(jsonObj.getLong("ts"));

                        //如果标记的是新访客，那么有可能状态是不准确的，需要修复
                        if (isNew.equals("1")) {
                            //从Flink状态中获取上次访问日期
                            String lastVisitDate = lastVisitDateState.value();
                            if (lastVisitDate != null && lastVisitDate.length() > 0) {
                                if (!lastVisitDate.equals(curDate)) {
                                    //说明已经访问过,进行状态的修复
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //说明该设备第一次访问，将当前访问日期放到状态中
                                lastVisitDateState.update(curDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );

        jsonObjWithFlagDS.print(">>>>>");


        //TODO 5.分流   启动---启动侧输出流   曝光---曝光侧输出流   页面---主流


        env.execute();
    }
}
