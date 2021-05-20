package com.me.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.me.gmall.realtime.app.func.DimSink;
import com.me.gmall.realtime.bean.TableProcess;
import com.me.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.me.gmall.realtime.app.func.TableProcessFunction;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Author: zs
 * Date: 2021/5/12
 * Desc: 业务数据的动态分流
 * 分流业务执行思路
 *  -需要启动的进行以及应用
 *       zk、kafka、hdfs、hbase、maxwell、BaseDBApp
 *  -业务数据库表发生了变化
 *  -变化信息会记录到binlog日志中
 *  -maxwell会从binlog中读取变化信息，发送到kafka的ods_base_db_m
 *  -BaseDBApp从kafka的ods_base_db_m主题中读取数据
 *  -对读取的数据进行结构转换并进行简单的ETL
 *  -使用FlinkCDC读取配置表的数据形成广播流
 *  -将主流和广播流进行连接
 *  -连接之后分别对两条流数据进行处理（单独封装类TableProcessFunction）
 *      >广播流
 *          &获取状态
 *          &如果读取的配置是维度配置，提前建立Phoenix表
 *          &将读取到的配置信息放到状态中
 *      >业务主流
 *          &获取状态
 *          &拼接key
 *          &根据key获取当前处理的数据的配置信息
 *          &过滤不需要向下游传递的字段
 *          &根据配置信息进行分流
 *              维度--侧输出流
 *              事实--主流
 *  -在BaseDBApp中获取相关流进行打印
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


       //5.4 获取维度侧输出流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print("主流>>>：");
        dimDS.print("维度###：");


        //TODO 6.将维度侧输出流数据保存到Phoenix表中
        dimDS.addSink(new DimSink());

        //TODO 7.将事实主流数据写回到kafka的dwd层
        realDS.addSink(
                MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String topic = jsonObj.getString("sink_table");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                        // 需要序列化
                        return new ProducerRecord<byte[], byte[]>(topic,dataJsonObj.toJSONString().getBytes());
                    }
                })
        );

        //提交job
        env.execute();
    }
}
