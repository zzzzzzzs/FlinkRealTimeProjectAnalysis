package com.me.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.utils.DimUtil;
import com.me.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * Author: zs
 * Date: 2021/5/17
 * Desc: 异步维度关联函数类
 * 模板方法设计模式
 *  在父类中定义实现某一个功能的核心算法的骨架，将具体实现延迟到子类中
 *  在不改变父类中核心算法骨架前提下，每一个子类中都可以有自己不同的实现。
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    // 面向接口编程
    private ExecutorService executorService;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    // TODO　？？？　这里的open是来一条执行一次吗，这里是如何执行的
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取线程池对象
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //发送异步请求
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                //获取key主键的值，由于泛型的原因主键key不知道类型，不用做具体的实现方式。具体实现延迟到子类创建的时候实现
                String key = getKey(obj);
                //获取维度对象
                JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);
                //维度关联
                if(dimJsonObj != null && dimJsonObj.size() > 0){
                    //由于泛型的原因，obj不知道类型，不用做具体的实现方式
                    join(obj,dimJsonObj);
                }
                long end = System.currentTimeMillis();
                System.out.println("异步维度查询耗时:" +(end - start) +"毫秒");
                //接收异步响应结果，向下游传递数据
                resultFuture.complete(Arrays.asList(obj));
            }
        });
    }


    // 这里的程序写成了一个接口，不用抽象方法了
//    public abstract void join(T obj, JSONObject dimJsonObj);
//    public abstract String getKey(T obj);

}

