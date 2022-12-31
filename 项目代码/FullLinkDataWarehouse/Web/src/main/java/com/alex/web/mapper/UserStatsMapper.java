package com.alex.web.mapper;

import com.alex.web.bean.UserChangeCtPerType;
import com.alex.web.bean.UserPageCt;
import com.alex.web.bean.UserTradeCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface UserStatsMapper {
    @Select("select 'home'  page_id,\n" +
            "       sum(home_uv_ct)        uvCt\n" +
            "       from dws_traffic_page_view_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'good_detail' page_id,\n" +
            "       sum(good_detail_uv_ct) uvCt\n" +
            "       from dws_traffic_page_view_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'cart' page_id,\n" +
            "       sum(cart_add_uu_ct) uvCt\n" +
            "       from dws_trade_cart_add_uu_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'trade' page_id,\n" +
            "       sum(order_unique_user_count) uvCt\n" +
            "       from dws_trade_order_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'payment' page_id,\n" +
            "       sum(payment_suc_unique_user_count) uvCt\n" +
            "       from dws_trade_payment_suc_window\n" +
            "       where toYYYYMMDD(stt) = #{date};")
    List<UserPageCt> selectUvByPage(@Param("date") Integer date);

    @Select("select 'backCt'          type,\n" +
            "       sum(back_ct)    back_ct\n" +
            "from dws_user_user_login_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select 'activeUserCt'     type,\n" +
            "       sum(uu_ct)      uu_ct\n" +
            "from dws_user_user_login_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "union all\n" +
            "select 'newUserCt' type,\n" +
            "       sum(register_ct) register_ct\n" +
            "from dws_user_user_register_window\n" +
            "where toYYYYMMDD(stt) = #{date};")
    List<UserChangeCtPerType> selectUserChangeCtPerType(@Param("date")Integer date);

    @Select("select 'order' trade_type,\n" +
            "       sum(order_new_user_count) order_new_user_count\n" +
            "       from dws_trade_order_window\n" +
            "       where toYYYYMMDD(stt) = #{date}\n" +
            "       union all\n" +
            "select 'payment' trade_type,\n" +
            "       sum(payment_new_user_count) pay_suc_new_user_count\n" +
            "       from dws_trade_payment_suc_window\n" +
            "       where toYYYYMMDD(stt) = #{date};")
    List<UserTradeCt> selectTradeUserCt(@Param("date")Integer date);
}
