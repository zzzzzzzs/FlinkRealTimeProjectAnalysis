package com.me.gmall.realtime.app.dws;


import com.me.gmall.realtime.bean.ProvinceStats;
import com.me.gmall.realtime.utils.ClickHouseUtil;
import com.me.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: zs
 * Date: 2021/5/22
 * Desc: 地区主题统计应用类
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.检查点 略

        //TODO 3.创建动态表 看官网 https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/common.html
        // 创建动态表，指定watermark以及提取事件时间，同时从kafka中消费数据
        String orderWideTopic = "dwm_order_wide";
        String groupId = "province_stats_group";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //TODO 4.分组、开窗、聚合计算
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code area_code," +
                "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                "from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 5.将动态表转化为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        //TODO 6.写到CK
        provinceStatsDS.print(">>>>");
        provinceStatsDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into  province_stats_1116  values(?,?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}

