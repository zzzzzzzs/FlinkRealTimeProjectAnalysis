package com.me.gmall.publisher.mapper;


import com.me.gmall.publisher.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: zs
 * Date: 2021/5/25
 * Desc: 访客主题统计Mapper接口
 */
public interface VisitorStatsMapper {

    //获取新老访客指标
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum  from visitor_stats_1116 where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> selectVisitorStatsByIsNew(Integer date);

    //访客分时统计查询
    @Select("select sum(if(is_new='1', visitor_stats_1116.uv_ct,0)) new_uv,toHour(stt) hr,sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct  from visitor_stats_1116 where toYYYYMMDD(stt)=#{date} group by toHour(stt)")
    List<VisitorStats> selectVisitorStatsByHour(Integer date);
}

