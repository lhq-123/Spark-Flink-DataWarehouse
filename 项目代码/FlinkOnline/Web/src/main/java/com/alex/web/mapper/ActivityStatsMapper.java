package com.alex.web.mapper;

import com.alex.web.bean.ActivityReduceStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ActivityStatsMapper {
    @Select("select sum(order_activity_reduce_amount)                   activity_reduce_amount,\n" +
            "       sum(order_origin_total_amount)                      origin_total_amount,\n" +
            "       round(round(toFloat64(activity_reduce_amount), 5) /\n" +
            "             round(toFloat64(origin_total_amount), 5), 20) subsidyRate\n" +
            "from dws_trade_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt);")
    List<ActivityReduceStats> selectActivityStats(@Param(value = "date")Integer date);
}
