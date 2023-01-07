package com.alex.web.mapper;

import com.alex.web.bean.TrafficVisitorStatsPerHour;
import com.alex.web.bean.TrafficVisitorTypeStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficVisitorStatsMapper {
    @Select("select\n" +
            "is_new,\n" +
            "sum(uv_ct) uv_ct,\n" +
            "sum(pv_ct) pv_ct,\n" +
            "sum(sv_ct) sv_ct,\n" +
            "sum(uj_ct) uj_ct,\n" +
            "sum(dur_sum) dur_sum\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) =#{date}\n" +
            "group by is_new")
    List<TrafficVisitorTypeStats> selectVisitorTypeStats(@Param("date")Integer date);

    @Select("select\n" +
            "toHour(stt) hr,\n" +
            "sum(uv_ct)  uv_ct,\n" +
            "sum(pv_ct)  pv_ct,\n" +
            "sum(if(is_new = '1', dws_traffic_vc_ch_ar_is_new_page_view_window.uv_ct, 0)) new_uv_ct\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
            "where toYYYYMMDD(stt) =#{date}\n" +
            "group by hr")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(Integer date);
}
