package com.me.gmall.realtime.utils;


import com.me.gmall.realtime.annotation.TransientSink;
import com.me.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author：zs
 * Date: 2021/5/21
 * Desc: 操作Click house的工具类，基于JDBC协议
 */
public class ClickHouseUtil {
    public static <T>SinkFunction<T> getJdbcSink(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //insert into visitor_stats_1116 values(?,?,?,?,?,?,?,?,?,?,?,?)
                        //使用反射的方式获取对象的所有属性
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < fieldArr.length; i++) {
                            //获取每一个属性对象
                            Field field = fieldArr[i];
                            field.setAccessible(true);

                            //判断当前属性是否被@TransientSink注解标记,如果有标记则不需要写入clickhouse
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if(transientSink != null){
                                skipNum ++;
                                continue;
                            }
                            try {
                                //获取属性的值
                                Object fieldValue = field.get(obj);
                                //将属性的值赋值给问号占位符
                                ps.setObject(i + 1 - skipNum,fieldValue);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
        return sinkFunction;
    }
}

