package com.me.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.common.GmallConfig;
import com.me.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author: zs
 * Date: 2021/5/14
 * Desc: 维度流sink函数
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取输出的Phoenix表名
        String tableName = jsonObj.getString("sink_table").toUpperCase();
        //获取data数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName,dataJsonObj);
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(upsertSql);
                ps.executeUpdate();
                // Phoenix需要自己提交事务
                conn.commit();
                System.out.println("向Phoenix中插入数据的SQL：" + upsertSql);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向Phoenix中插入数据失败");
            } finally {
                if(ps != null){
                    ps.close();
                }
            }
        }

        if(jsonObj.getString("type").equals("update")||jsonObj.getString("type").equals("delete")){
            //清空当前数据在Redis中的缓存
            DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
        }
    }

    //拼接插入到Phoenix表的SQL
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        String upsertSql = "upsert into "+GmallConfig.HBASE_SCHEMA
                +"."+tableName
                +" ("+ StringUtils.join(dataJsonObj.keySet(),",") +") " +
                " values(\'"+StringUtils.join(dataJsonObj.values(),"\',\'")+"\')";
        return upsertSql;
    }
}

