package com.me.gmall.realtime.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: zs
 * Desc:日期转换工具类
 * JDK8的DateTimeFormatter替换SimpleDateFormat，因为SimpleDateFormat存在线程安全问题
 * 同时修改同一个变量会出现线程安全
 */
public class DateTimeUtil {
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // 不好用，总是说格式不正确
    public static long toTS(String date) {
        System.out.println("输入的数据为：" + date);
        // 这里就是
        Date ts = null;
        try {
            ts = sdf.parse(date);
        } catch (ParseException  e) {
            e.printStackTrace();
            throw new RuntimeException("错误数据为：" + date);
        }
        return ts.getTime();
    }

    public final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    //将字符串类型日期转换为时间戳
    public static long toTs(String dateTimeStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeStr,dtf);
        Instant instant = localDateTime.toInstant(ZoneOffset.of("+8"));
        return instant.toEpochMilli();
    }

    //将日期类型的对象转换为字符串
    public static String toYMDHms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        String dateStr = localDateTime.format(dtf);
        return dateStr;
    }
    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }
}
