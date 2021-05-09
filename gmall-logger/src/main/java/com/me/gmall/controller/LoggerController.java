package com.me.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: zs
 * Date: 2021/5/9
 * Desc: 日志处理程序
 1.离线和实时架构对比

 2.编写SpringBoot程序，处理日志
 -打印输出
 -落盘
 logback
 -发送到Kafka的主题
 KafkaTemplate
 3.处理生成日志思路
 发送数据----window上采集服务
 发送数据----打包到单台Linux
 发送数据----到集群
 使用Nginx做负载均衡
 location /{
 proxy_pass http://反向代理服务器;
 }
 upstream 反向代理服务器{
 server 202;
 server 203;
 server 204;
 }
 4.测试思路
 */
@RestController
@Slf4j
public class LoggerController {

    //Spring提供的对Kafka进行操作的模板类
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    //@Slf4j注解底层自动生成以下代码
    //private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LoggerController.class);


    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String logStr){
        //1.输出到控制台
        //System.out.println(logStr);
        //2.落盘
        log.info(logStr);
        //3.发送到Kafka指定的主题中
        kafkaTemplate.send("ods_base_log",logStr);
        return "success";
    }
}
