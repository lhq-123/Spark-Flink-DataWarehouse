package com.alex.web.mapper;

import com.alex.web.bean.CouponReduceStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CouponStatsMapper {
    @Select("select sum(order_coupon_reduce_amount)                     coupon_reduce_amount,\n" +
            "       sum(order_origin_total_amount)                      origin_total_amount,\n" +
            "       round(round(toFloat64(coupon_reduce_amount), 5) /\n" +
            "             round(toFloat64(origin_total_amount), 5), 20) coupon_subsidy_rate\n" +
            "from dws_trade_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt);")
    List<CouponReduceStats> selectCouponStats(@Param("date")Integer date);
}
