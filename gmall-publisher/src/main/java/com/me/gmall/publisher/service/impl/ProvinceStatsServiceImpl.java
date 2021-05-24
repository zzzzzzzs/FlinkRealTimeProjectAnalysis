package com.me.gmall.publisher.service.impl;

import com.me.gmall.publisher.bean.ProvinceStats;
import com.me.gmall.publisher.mapper.ProvinceStatsMapper;
import com.me.gmall.publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc: 地区主题统计Service接口实现类
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(Integer date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}

