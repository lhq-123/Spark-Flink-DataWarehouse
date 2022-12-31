package com.alex.web.service;

import com.alex.web.bean.TrafficVisitorStatsPerHour;
import com.alex.web.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorStatsService {
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);

    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}
