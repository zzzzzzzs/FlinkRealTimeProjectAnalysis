package com.me.gmall.publisher.mapper;


import com.me.gmall.publisher.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Felix
 * Date: 2021/5/24
 * Desc: 商品统计mapper接口
 */
public interface ProductStatsMapper {
    //获取总成交金额
    @Select("select sum(order_amount) order_amount from product_stats_1116 where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGMV(Integer date);

    //获取某天品牌交易额排行
    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats_1116  where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByTm(@Param("date") Integer date, @Param("limit") Integer limit);

    //获取某天品类交易额排行
    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats_1116  where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsByCategory3(@Param("date") Integer date,@Param("limit") Integer limit);

    //获取某天SPU交易额排行
    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(order_ct) order_ct from product_stats_1116  where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name having order_amount > 0 order by order_amount desc limit #{limit}")
    List<ProductStats> selectProductStatsBySpu(@Param("date") Integer date,@Param("limit") Integer limit);
}