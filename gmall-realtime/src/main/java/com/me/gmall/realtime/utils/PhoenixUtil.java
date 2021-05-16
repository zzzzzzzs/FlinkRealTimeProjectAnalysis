package com.me.gmall.realtime.utils;


import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/15
 * Desc: 操作Phoenix的工具类
 */
public class PhoenixUtil {

    private static Connection conn;

    //连接初始化
    private static void queryInit() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        conn.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    //从Phoenix中查询数据，泛型
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> resList = new ArrayList<>();
        try {
            if (conn == null) {
                queryInit();
            }
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //执行SQL语句
            rs = ps.executeQuery();

            //获取结果集对象元数据信息
            ResultSetMetaData metaData = rs.getMetaData();

            //处理结果集
            while (rs.next()) {
                // TODO 通过反射创建封装的对象
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,value);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Phoenix中查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) {
        List<JSONObject> list = queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
        System.out.println(list);
    }
}

