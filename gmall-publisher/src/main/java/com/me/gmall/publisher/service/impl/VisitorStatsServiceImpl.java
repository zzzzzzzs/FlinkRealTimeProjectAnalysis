package com.me.gmall.publisher.service.impl;


import com.me.gmall.publisher.bean.VisitorStats;
import com.me.gmall.publisher.mapper.VisitorStatsMapper;
import com.me.gmall.publisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/25
 * Desc:  访客主题统计接口实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByIsNew(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByIsNew(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }
}
