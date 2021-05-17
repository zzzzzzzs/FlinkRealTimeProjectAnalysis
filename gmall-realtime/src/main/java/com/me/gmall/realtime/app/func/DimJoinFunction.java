package com.me.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;

/**
 * Author: zs
 * Date: 2021/5/17
 * Desc: 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj);

    String getKey(T obj);
}
