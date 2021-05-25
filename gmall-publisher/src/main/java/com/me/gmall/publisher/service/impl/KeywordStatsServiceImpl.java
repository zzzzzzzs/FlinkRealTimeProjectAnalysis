package com.me.gmall.publisher.service.impl;


import com.me.gmall.publisher.bean.KeywordStats;
import com.me.gmall.publisher.mapper.KeywordStatsMapper;
import com.me.gmall.publisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/25
 * Desc:
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(Integer date, Integer limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}

