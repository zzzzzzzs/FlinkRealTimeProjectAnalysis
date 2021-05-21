package com.me.gmall.realtime.common;
/**
 * Author: zs
 * Date: 2021/5/13
 * Desc: 项目配置常量类
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA="Flink_REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";
}
