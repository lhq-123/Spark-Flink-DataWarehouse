package com.alex.web.mapper;

import com.alex.web.bean.TradeProvinceOrderAmount;
import com.alex.web.bean.TradeProvinceOrderCt;
import com.alex.web.bean.TradeStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeStatsMapper {
    @Select("select sum(order_amount) order_total_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt);")
    Double selectTotalAmount(@Param("date")Integer date);

    @Select("select '下单数' type,\n" +
            "       sum(order_count)        value\n" +
            "from dws_trade_trademark_category_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '下单人数' type,\n" +
            "       count(distinct user_id) value\n" +
            "from dws_trade_trademark_category_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '退单数' type,\n" +
            "       sum(refund_count)       value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select '退单人数' type,\n" +
            "       count(distinct user_id) value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            "where toYYYYMMDD(stt) = #{date};")
    List<TradeStats> selectTradeStats(@Param("date")Integer date);

    @Select("select province_name,\n" +
            "       sum(order_count)  order_count\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by province_id, province_name;")
    List<TradeProvinceOrderCt> selectTradeProvinceOrderCt(@Param("date")Integer date);

    @Select("select province_name,\n" +
            "       sum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "and province_name is not null \n" +
            "and province_name !='' \n" +
            "group by province_id, province_name;")
    List<TradeProvinceOrderAmount> selectTradeProvinceOrderAmount(@Param("date")Integer date);
}
