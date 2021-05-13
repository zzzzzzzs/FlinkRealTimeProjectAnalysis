package com.me.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.me.gmall.realtime.bean.TableProcess;
import com.me.gmall.realtime.func.MyDeserializationSchemaFunction;
import com.me.gmall.realtime.func.TableProcessFunction;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * Author: zs
 * Date: 2021/5/12
 * Desc: 业务数据的动态分流
 */
public class BaseDBapp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //TODO 2.设置检查点
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3 设置取消job是否保存检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck"));
        //2.6 指定操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 3.从kafka中读取数据
        //定义消费主题以及消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        //3.1 获取kakfaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.2 通过kafkaSource消费数据，并封装到流中
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //3.3 对流中的数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //3.4 对流中的数据进行ETL，ETL的功能不固定
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                // TODO 将table为空的去掉
                boolean flag = jsonObj.getString("table") != null
                        && jsonObj.getString("table").length() > 0
                        && jsonObj.getJSONObject("data") != null
                        && jsonObj.getString("data").length() > 5;
                return flag;
            }
        });

        //TODO 4.使用FlinkCDC读取配置表数据
        //4.1 创建MySQLSourceFunction
        DebeziumSourceFunction<String> sourceFunction =
                MySQLSource.<String>builder()
                        .hostname("hadoop102")
                        .port(3306)
                        .username("root")
                        .password("000000")
                        .databaseList("gmallFlinkRealTimeDIM")
                        .tableList("gmallFlinkRealTimeDIM.table_process")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new MyDeserializationSchemaFunction())
                        .build();

        //4.2 读取数据封装为流
        DataStreamSource<String> mySqlDS = env.addSource(sourceFunction);

        //4.3 定义广播状态描述器
        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcess>("table_process", String.class, TableProcess.class);

        //4.4 将读取的配置信息流转换为广播流
        BroadcastStream<String> broadcastDS = mySqlDS.broadcast(mapStateDescriptor);


        //TODO 5.连接主流以及配置广播流
        //5.1 连接两条流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //5.2 定义侧输出流标记
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };

        //5.3 对数据进行分流处理   维度---侧输出流  事实---主流
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
                new TableProcessFunction(dimTag, mapStateDescriptor)
        );

//        realDS.print();

       /* //5.4 获取维度侧输出流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print(">>>");
        dimDS.print("####");*/

        //提交job
        env.execute();
    }
}
