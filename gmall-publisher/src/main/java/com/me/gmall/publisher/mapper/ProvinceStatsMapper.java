package com.me.gmall.publisher.mapper;

import com.me.gmall.publisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/24
 * Desc: 地区主题统计Mapper接口
 */
public interface ProvinceStatsMapper {

    @Select("select province_id,province_name,sum(order_amount) order_amount from province_stats_1116  where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(Integer date);
}

