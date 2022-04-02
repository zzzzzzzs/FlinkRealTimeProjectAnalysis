package com.me.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Author: zs
 * Date: 2021/5/17
 * Desc: 获取Jedis工具类
 */
public class RedisUtil {
    // 使用线程池 懒汉式--有线程安全的问题
    private static JedisPool jedisPool;

    public static void init() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100); //最大可用连接数
        jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
        jedisPoolConfig.setMaxWaitMillis(2000); //等待时间

        jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
        jedisPoolConfig.setMinIdle(5); //最小闲置连接数

        jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong
        System.out.println("----初始化连接池---");
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,20000);
    }
    public static Jedis getJedis(){
        if(jedisPool == null){
            init();
        }
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }


    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String msg = jedis.ping();
        System.out.println(msg);
    }
}

