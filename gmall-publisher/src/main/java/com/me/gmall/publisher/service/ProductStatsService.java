package com.me.gmall.publisher.service;


import com.me.gmall.publisher.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc: 商品主题统计业务接口
 */
public interface ProductStatsService {
    //获取某天交易总额
    BigDecimal getGMV(Integer date);

    //获取某天品牌交易额排行
    List<ProductStats> getProductStatsByTm(Integer date,Integer limit);

    //获取某天品类交易额排行
    List<ProductStats> getProductStatsByCategory3(Integer date,Integer limit);

    //获取某天SPU交易额排行
    List<ProductStats> getProductStatsBySpu(Integer date,Integer limit);
}
