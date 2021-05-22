package com.me.gmall.realtime.app.dws;

import com.me.gmall.realtime.app.func.KeywordUDTF;
import com.me.gmall.realtime.bean.KeywordStats;
import com.me.gmall.realtime.common.GmallConstant;
import com.me.gmall.realtime.utils.ClickHouseUtil;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: zs
 * Date: 2021/5/22
 * Desc: 关键词统计应用
 */
public class KeywordStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.创建动态表 看官网 https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/common.html
        // 指定watermark以及提取事件时间，同时从kafka中消费数据
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_group";

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '3' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //TODO 3.注册自定义UDTF函数，自定义分词器
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 4.过滤数据，选出搜索商品且过滤不为空的数据
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        /* TODO 5.使用自定义UDTF函数进行分词    https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/sql/queries.html#joins
            将分开的词与时间戳进行join连接
            类似：
                小米  54564564564646
                银灰  54564564564646
                128G  54564564564646
        */
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //TODO 6.分组、开窗、聚合计算
        Table keywordStatsSearch  = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   "+keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //TODO 7.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        //TODO 8.将流中的数据写到CK中
        keywordStatsDS.print(">>>>");
        keywordStatsDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats_1116(keyword,ct,source,stt,edt,ts)" +
                        " values(?,?,?,?,?,?)")
        );


        env.execute();


    }
}

