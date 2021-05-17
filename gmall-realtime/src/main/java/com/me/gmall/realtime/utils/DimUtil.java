package com.me.gmall.realtime.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/17
 * Desc: 查询维度的工具类
 */
public class DimUtil {

    //直接从Phoenix中查询维度数据，没有使用缓存优化，有可能指定多个字段，使用...的方式
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
        StringBuilder querySql = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            if (i > 0) {
                querySql.append(" and ");
            }

            querySql.append(columnName + " = '" + columnValue + "'");
        }
        System.out.println("查询维度的sql为：" + querySql);

        //底层调用PhoenixUtil，到Phoenix中查询数据
        List<JSONObject> dimJsonObjectList = PhoenixUtil.queryList(querySql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;
        if (dimJsonObjectList != null && dimJsonObjectList.size() > 0) {
            // 一般情况下按照id查询数据只能查出一条，所以就取出第一个了。正常情况下应该使用List返回
            dimJsonObj = dimJsonObjectList.get(0);
        } else {
            System.out.println("没有查询到维度数据:" + querySql);
        }

        return dimJsonObj;
    }

    // 提高代码的使用性，使用方便
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    /*
        从Phoenix中查询维度数据，使用旁路缓存优化
        先从缓存中查询维度数据，如果缓存中有，直接将维度数据返回；如果缓存中没有维度数据，再到Phoenix中
        查询维度数据，然后将查询出来的维度数据放到缓存中，下次直接从缓存中获取。
        第一次查询速度慢，但是当数据存入到redis中以后速度就快了。
    TODO 缓存选型：Redis
        dim:dim_user_info:10_zs
        type:String     key: dim:表名:主键值1_主键值2       expire:1day
    */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接查询维度的sql
        StringBuilder querySql = new StringBuilder("select * from " + tableName + " where ");
        //拼接操作Redis的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            if (i > 0) {
                querySql.append(" and ");
                redisKey.append("_");
            }

            querySql.append(columnName + " = '" + columnValue + "'");
            redisKey.append(columnValue);
        }

        //从redis缓存中查询维度数据
        Jedis jedis = null;
        String dimJsonStr = null;
        JSONObject dimJsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询缓存失败");
        }

        //判断是否从redis缓存中查到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            //查到了：  直接将json字符串转换为json对象
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //没查到： 需要从Phoenix中查询
            System.out.println("查询维度的sql为：" + querySql);
            //底层调用PhoenixUtil，到Phoenix中查询数据
            List<JSONObject> dimJsonObjectList = PhoenixUtil.queryList(querySql.toString(), JSONObject.class);

            if (dimJsonObjectList != null && dimJsonObjectList.size() > 0) {
                dimJsonObj = dimJsonObjectList.get(0);
                //从Phoenix中查询数据后，放到Redis中进行缓存
                if (jedis != null) {
                    // 设置一天的过期时间
                    jedis.setex(redisKey.toString(), 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("没有查询到维度数据:" + querySql);
            }
        }

        //关闭连接
        if (jedis != null) {
            jedis.close();
            System.out.println("关闭缓存连接 ");
        }
        return dimJsonObj;
    }

    //让缓存失效的方法
    public static void deleteCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        // 未优化的方式，速度很慢 15742毫秒
//        JSONObject dimJsonObj = getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "13"), Tuple2.of("tm_name", "bb"));
        // 优化的方式，使用了1705毫秒
        JSONObject dimJsonObj = getDimInfo("DIM_BASE_TRADEMARK", Tuple2.of("id", "13"));
//        JSONObject dimJsonObj = getDimInfo("DIM_BASE_TRADEMARK", "13");
        long end = System.currentTimeMillis();
        System.out.println(dimJsonObj);
        System.out.println("使用了" + (end - start) + "毫秒");
    }
}

