package com.me.gmall.publisher.service;



import com.me.gmall.publisher.bean.KeywordStats;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/25
 * Desc: 关键词主题统计Service接口
 */
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(Integer date, Integer limit);
}
