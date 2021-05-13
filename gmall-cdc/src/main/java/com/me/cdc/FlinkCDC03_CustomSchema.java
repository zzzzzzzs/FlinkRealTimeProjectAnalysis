package com.me.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/12
 * Desc: 自定义反序列方式
 */
public class FlinkCDC03_CustomSchema {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备流处理环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 可是使用本地的webUI看，需要添加依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction =
                MySQLSource.<String>builder()
                        .hostname("hadoop102")
                        .port(3306)
                        .username("root")
                        .password("000000")
                        .databaseList("gmallFlinkRealTimeDIM")
                        .tableList("gmallFlinkRealTimeDIM.t_user")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new MySchema())
                        .build();

        DataStreamSource<String> mySqlDS = env.addSource(sourceFunction);

        mySqlDS.print(">>>>");
        env.execute();
    }
}

class MySchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> out) throws Exception {
        //获取valueStruct
        Struct valueStruct = (Struct) sourceRecord.value();

        //获取sourceStruct  source=Struct{db=gmall1116_realtime,table=t_user}
        Struct sourceStruct = valueStruct.getStruct("source");

        //获取数据库名称
        String dbName = sourceStruct.getString("db");
        //获取表名称
        String tableName = sourceStruct.getString("table");
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        //获取afterStruct中所有属性  并封装为dataJson
        JSONObject dataJsonObj = new JSONObject();
        //获取afterStruct after=Struct{id=1,name=zs,age=18}
        Struct afterStruct = valueStruct.getStruct("after");
        if(afterStruct!=null){
            List<Field> fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                dataJsonObj.put(field.name(), afterStruct.get(field));
            }
        }

        //将库名、表名以及操作类型和具体数据 封装为一个大的json
        JSONObject resJsonObj = new JSONObject();
        resJsonObj.put("database", dbName);
        resJsonObj.put("table", tableName);
        resJsonObj.put("type", type);
        resJsonObj.put("data", dataJsonObj);

        out.collect(resJsonObj.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
