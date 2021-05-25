package com.me.gmall.publisher.service;



import com.me.gmall.publisher.bean.VisitorStats;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/25
 * Desc: 访客主题统计Service接口
 */
public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByIsNew(Integer date);

    List<VisitorStats> getVisitorStatsByHour(Integer date);
}
