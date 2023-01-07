package com.alex.web.service;

import com.alex.web.bean.TradeProvinceOrderAmount;
import com.alex.web.bean.TradeProvinceOrderCt;
import com.alex.web.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}
