package com.alex.web.mapper;

import com.alex.web.bean.TrafficKeywords;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficKeywordsMapper {
    @Select("select keyword,\n" +
            "       sum(keyword_count * multiIf(\n" +
            "               source = 'SEARCH', 10,\n" +
            "               source = 'ORDER', 5,\n" +
            "               source = 'CART', 2,\n" +
            "               source = 'CLICK', 1, 0\n" +
            "           ))          keyword_score\n" +
            "from dws_traffic_source_keyword_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by toYYYYMMDD(stt), keyword\n" +
            "order by keyword_score desc;")
    List<TrafficKeywords> selectKeywords(@Param(value = "date") Integer date);
}
