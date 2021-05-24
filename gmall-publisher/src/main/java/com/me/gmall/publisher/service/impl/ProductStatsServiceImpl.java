package com.me.gmall.publisher.service.impl;


import com.me.gmall.publisher.bean.ProductStats;
import com.me.gmall.publisher.mapper.ProductStatsMapper;
import com.me.gmall.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc: 商品主题统计业务实现类
 */
//将对象的管理交给Spring的IOC容器（原来对象的控制权力在程序员的手中，现在反转到框架手中）
@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired //注入
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return productStatsMapper.selectGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTm(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTm(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategory3(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySpu(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySpu(date,limit);
    }
}

