package com.alex.web.service.impl;

import com.alex.web.bean.TrafficKeywords;
import com.alex.web.mapper.TrafficKeywordsMapper;
import com.alex.web.service.TrafficKeywordsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficKeywordsServiceImpl implements TrafficKeywordsService {

    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;

    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}
