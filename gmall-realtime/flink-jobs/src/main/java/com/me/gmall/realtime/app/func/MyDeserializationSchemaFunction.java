package com.me.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/13
 * Desc: 自定义反序列化器
 */
public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {

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
