package com.me.gmall.publisher.service;



import com.me.gmall.publisher.bean.ProvinceStats;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc:
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(Integer date);
}
