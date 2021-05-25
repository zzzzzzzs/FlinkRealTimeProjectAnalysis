package com.me.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

import static com.me.gmall.realtime.common.KafkaConfig.*;

/**
 * Author: zs
 * Date: 2021/5/11
 * Desc: 日志数据的分流
 * Flink的状态
 * -算子状态
 * -键控状态
 * 执行流程说明
 * -运行模拟生成日志的jar
 * -将日志发送到nginx
 * -nginx将请求转发给202、203、204上的日志采集服务器
 * -202、203、204上的日志采集服务器 接收到日志数据之后，进行打印到控制台、落盘、发送到ods_base_log
 * -运行的BaseLogApp应用从ods_base_log中读取数据
 *      >结构转换
 *      >状态修复
 *      >分流
 * -将分流之后的数据写到kafka的dwd主题中
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
//        //TODO 2.设置检查点
//        //2.1 每隔5秒创建一次检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //2.3 设置取消job后是否保留检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置检查点重启策略，如果重启失败，需要自己看日志检查错误
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
//        //2.5 设置状态后端   内存|文件系统|RocksDB
//        // TODO 这里在HDFS路径中的chk-3是checkpoint的文件，会5秒变一次内容
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmallFlinkRealTime/ck"));
//        //2.6 设置hadoop的用户，否则用户名是电脑的用户，没有权限
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组

        //3.2 通过工具类  获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(ODSTOPIC, ODSGROUPID);

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
        //5.1 定义侧输出流标记
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        //5.2分流
        SingleOutputStreamOperator<String> splitDS =
                jsonObjWithFlagDS.process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                        //获取启动属性
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        //将接收到的jsonObj转换为字符串
                        String jsonStr = jsonObj.toJSONString();
                        //判断是否为启动日志
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            //是启动日志   将启动日志放到启动侧输出流中
                            ctx.output(startTag, jsonStr);
                        } else {
                            //如果不是启动日志的话，都属于页面日志类型
                            out.collect(jsonStr);
                            //同时在页面日志中，还包含曝光日志
                            JSONArray displaysArr = jsonObj.getJSONArray("displays");
                            //判断是否为曝光日志
                            if (displaysArr != null && displaysArr.size() > 0) {
                                //获取页面id
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                //对曝光数组进行遍历
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displaysJsonObj = displaysArr.getJSONObject(i);
                                    //给曝光的json对象补充pageId
                                    displaysJsonObj.put("page_id", pageId);
                                    ctx.output(displayTag, displaysJsonObj.toJSONString());
                                }
                            }
                        }
                    }
                });

        //5.3 获取各个流数据并输出测试
        DataStream<String> startDS = splitDS.getSideOutput(startTag);
        DataStream<String> displayDS = splitDS.getSideOutput(displayTag);

        splitDS.print(">>>页面>>>");
        startDS.print("###启动###");
        displayDS.print("$$$曝光$$$");

        //TODO 6. 将不同流的数据写到Kafka的dwd的不同主题中
        //6.1 声明kafka主题

        //6.2 sink操作
        startDS.addSink(MyKafkaUtil.getKafkaSink(STARTSINKTOPIC));
        displayDS.addSink(MyKafkaUtil.getKafkaSink(DISPLAYSINKTOPIC));
        splitDS.addSink(MyKafkaUtil.getKafkaSink(PAGESINKTOPIC));

        env.execute();
    }
}
