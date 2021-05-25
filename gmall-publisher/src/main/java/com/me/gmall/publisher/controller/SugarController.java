package com.me.gmall.publisher.controller;


import com.me.gmall.publisher.bean.KeywordStats;
import com.me.gmall.publisher.bean.ProductStats;
import com.me.gmall.publisher.bean.ProvinceStats;
import com.me.gmall.publisher.bean.VisitorStats;
import com.me.gmall.publisher.service.KeywordStatsService;
import com.me.gmall.publisher.service.ProductStatsService;
import com.me.gmall.publisher.service.ProvinceStatsService;
import com.me.gmall.publisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc: 大屏展示的控制类
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    @Autowired
    VisitorStatsService visitorStatsService;

    @Autowired
    KeywordStatsService keywordStatsService;


    // 3D词云
    @RequestMapping("/keyword")
    public String getKeywordStats(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) date = now();

        List<KeywordStats> keywordStatsList = keywordStatsService.getKeywordStats(date, limit);
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": [");
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats = keywordStatsList.get(i);
            jsonB.append("{\"name\": \""+keywordStats.getKeyword()+"\",\"value\": "+keywordStats.getCt()+"}");
            if(i < keywordStatsList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("]}");
        return jsonB.toString();
    }

    // 折线图
    @RequestMapping("/hour")
    public String getVisitorStatsByHour(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0) date =now();

        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsByHour(date);
        //定义一个数组，用于存放0~23点的数据
        VisitorStats[] visitorStatsArr = new VisitorStats[24];
        //遍历访客统计集合
        for (VisitorStats visitorStats : visitorStatsList) {
            //将遍历的结果赋给数组中的元素
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }
        //定义几个List集合，分别用来存放小时、pv、uv、newUv
        List<String> hrList = new ArrayList<>();
        List<Long> pvList = new ArrayList<>();
        List<Long> uvList = new ArrayList<>();
        List<Long> newMidUvList = new ArrayList<>();

        //遍历数组，得到0~23点的数据
        for (int hr = 0; hr < visitorStatsArr.length; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            //判断当前小时是否有访问
            if(visitorStats!=null){
                pvList.add(visitorStats.getPv_ct());
                uvList.add(visitorStats.getUv_ct());
                newMidUvList.add(visitorStats.getNew_uv());
            }else{
                pvList.add(0L);
                uvList.add(0L);
                newMidUvList.add(0L);
            }
            hrList.add(String.format("%02d",hr));
        }

        //拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidUvList,",") +"]}]}}";
        return  json;

    }

    // 表格
    @RequestMapping("/visitor")
    public String getVisitorStatsByIsNew(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0) date =now();
        //通过service获取访客统计相关度量值
        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsByIsNew(date);

        VisitorStats oldVisitorStats = new VisitorStats();
        VisitorStats newVisitorStats = new VisitorStats();
        for (VisitorStats visitorStats : visitorStatsList) {
            if(visitorStats.getIs_new().equals("1")){
                newVisitorStats = visitorStats;
            }else{
                oldVisitorStats = visitorStats;
            }
        }
        //把数据拼接入字符串
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新用户\",\"id\":\"new\"}," +
                "{\"name\":\"老用户\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"用户数(人)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"总访问页面(次)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"跳出率(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"平均访问页面数(人次)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return json;

    }

    // 地图
    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) date = now();
        //调用service获取地区以及对应的交易额
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceStatsList.size(); i++) {
            if (i > 0) {
                jsonB.append(",");
            }
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonB.append("{\"name\": \"" + provinceStats.getProvince_name() +
                    "\",\"value\": " + provinceStats.getOrder_amount() + "}");
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }

    // 轮播表格
    @RequestMapping("/spu")
    public Object getProductStatsBySpu(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) date = now();
        List<ProductStats> productStatsList = productStatsService.getProductStatsBySpu(date, limit);

        Map resMap = new HashMap();
        resMap.put("status", 0);

        Map dataMap = new HashMap();
        List columnList = new ArrayList();

        Map nameMap = new HashMap();
        nameMap.put("name", "商品名称");
        nameMap.put("id", "name");
        columnList.add(nameMap);

        Map amountMap = new HashMap();
        amountMap.put("name", "交易额");
        amountMap.put("id", "amount");
        columnList.add(amountMap);

        Map ctMap = new HashMap();
        ctMap.put("name", "订单数");
        ctMap.put("id", "ct");
        columnList.add(ctMap);

        dataMap.put("columns", columnList);


        List rowsList = new ArrayList();
        for (ProductStats productStats : productStatsList) {
            Map rowMap = new HashMap();
            rowMap.put("name", productStats.getSpu_name());
            rowMap.put("amount", productStats.getOrder_amount());
            rowMap.put("ct", productStats.getOrder_ct());
            rowsList.add(rowMap);
        }
        dataMap.put("rows", rowsList);

        resMap.put("data", dataMap);
        return resMap;
    }

    // 饼状图
    @RequestMapping("/category3")
    public Object getProductStatsByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) date = now();
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);

        /*{
            "status": 0,
            "data": [
            {
                "name": "PC",
                "value": 97
            },
            {
                "name": "iOS",
                "value": 50
            }
         ]
        }*/
        Map resMap = new HashMap<>();
        List dataList = new ArrayList();
        resMap.put("status", 0);
        for (ProductStats productStats : productStatsList) {
            Map dataMap = new HashMap();
            dataMap.put("name", productStats.getCategory3_name());
            dataMap.put("value", productStats.getOrder_amount());
            dataList.add(dataMap);
        }
        resMap.put("data", dataList);
        return resMap;
    }
    /*@RequestMapping("/category3")
    public String getProductStatsByCategory3(
        @RequestParam(value = "date",defaultValue = "0") Integer date,
        @RequestParam(value = "limit",defaultValue = "20") Integer limit){
        if(date ==0) date = now();
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": [");

        for (int i = 0; i < productStatsList.size(); i++) {
            ProductStats productStats = productStatsList.get(i);
            jsonB.append("{\"name\": \""+productStats.getCategory3_name()+"\"," +
                "\"value\": "+productStats.getOrder_amount()+"}");
            if(i < productStatsList.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("]}");
        return jsonB.toString();
    }*/

    // 柱状图
    @RequestMapping("/trademark")
    public String getProductStatsByTm(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "20") Integer limit
    ) {
        if (date == 0) date = now();
        //调用service方法获取品牌交易额排行
        List<ProductStats> productStatsList = productStatsService.getProductStatsByTm(date, limit);
        List<String> tmNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();
        for (ProductStats productStats : productStatsList) {
            tmNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }
        String json = "{\"status\":0,\"data\":" +
                "{\"categories\":[\"" + StringUtils.join(tmNameList, "\",\"") + "\"]," +
                "\"series\":[{\"name\":\"商品品牌\"," +
                "\"data\":[" + StringUtils.join(amountList, ",") + "]}]}}";
        return json;
    }

    // 翻牌器
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        //如果没有传递日期参数
        if (date == 0) {
            //将日期设置为当前日期
            date = now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{\"status\": 0,\"data\": " + gmv + "}";
        return json;
    }

    @RequestMapping("/test")
    public String test() {
        return "Hello";
    }

    //获取当前日期
    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

}

