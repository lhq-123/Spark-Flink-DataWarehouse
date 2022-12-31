package com.alex.web.mapper;

import com.alex.web.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface TrafficChannelStatsMapper {
    @Select("select ch,\n" +
            "       sum(uv_ct)           uv_ct\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), ch\n" +
            "order by uv_ct desc;")
    List<TrafficUvCt> selectUvCt(@Param("date")Integer date);

    @Select("select ch,\n" +
            "       sum(sv_ct)           sv_ct\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), ch\n" +
            "order by sv_ct desc;")
    List<TrafficSvCt> selectSvCt(@Param("date")Integer date);

    @Select("select ch,\n" +
            "       sum(pv_ct) / sum(sv_ct)   pv_per_session\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), ch\n" +
            "order by pv_per_session desc;")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date")Integer date);

    @Select("select ch,\n" +
            "       sum(dur_sum) / sum(sv_ct) dur_per_session\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), ch\n" +
            "order by dur_per_session desc;")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date")Integer date);

    @Select("select ch,\n" +
            "       sum(uj_ct) / sum(sv_ct)   uj_rate\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), ch\n" +
            "order by uj_rate desc;")
    List<TrafficUjRate> selectUjRate(@Param("date")Integer date);
}
