package com.alex.web.controller;

import com.alex.web.bean.TradeProvinceOrderAmount;
import com.alex.web.bean.TradeProvinceOrderCt;
import com.alex.web.bean.TradeStats;
import com.alex.web.service.TradeStatsService;
import com.alex.web.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/alex/realtime/trade")
public class TradeController {

    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/total")
    public String getTotalAmount(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        Double totalAmount = tradeStatsService.getTotalAmount(date);

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + totalAmount + "\n" +
                "}";
    }

    @RequestMapping("/stats")
    public String getStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeStats> tradeStatsList = tradeStatsService.getTradeStats(date);
        if (tradeStatsList == null) {
            return "";
        }

        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < tradeStatsList.size(); i++) {
            TradeStats tradeStats = tradeStatsList.get(i);
            String type = tradeStats.getType();
            Integer orderCt = tradeStats.getOrderCt();
            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"value\": " + orderCt + "\n" +
                    "}\n");
            if (i < tradeStatsList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"指标类型\",\n" +
                "        \"id\": \"type\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"度量值\",\n" +
                "        \"id\": \"value\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}\n";
    }

    @RequestMapping("/provinceOrderCt")
    public String getProvinceOrderCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeProvinceOrderCt> tradeProvinceOrderCtList = tradeStatsService.getTradeProvinceOrderCt(date);
        if (tradeProvinceOrderCtList == null) {
            return "";
        }
        StringBuilder mapData = new StringBuilder("[");
        for (int i = 0; i < tradeProvinceOrderCtList.size(); i++) {
            TradeProvinceOrderCt tradeProvinceOrderCt = tradeProvinceOrderCtList.get(i);
            String provinceName = tradeProvinceOrderCt.getProvinceName();
            Integer orderCt = tradeProvinceOrderCt.getOrderCt();
            mapData.append("{\n" +
                    "        \"name\": \"" + provinceName + "\",\n" +
                    "        \"value\": " + orderCt + "\n" +
                    "      }");
            if (i < tradeProvinceOrderCtList.size() - 1) {
                mapData.append(",");
            } else {
                mapData.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"mapData\": " + mapData + ",\n" +
                "    \"valueName\": \"订单数\"\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/provinceOrderAmount")
    public String getProvinceOrderAmount(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TradeProvinceOrderAmount> tradeProvinceOrderAmountList = tradeStatsService.getTradeProvinceOrderAmount(date);
        if (tradeProvinceOrderAmountList == null) {
            return "";
        }
        StringBuilder mapData = new StringBuilder("[");
        for (int i = 0; i < tradeProvinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount tradeProvinceOrderAmount = tradeProvinceOrderAmountList.get(i);
            String provinceName = tradeProvinceOrderAmount.getProvinceName();
            Double orderAmount = tradeProvinceOrderAmount.getOrderAmount();
            mapData.append("{\n" +
                    "        \"name\": \"" + provinceName + "\",\n" +
                    "        \"value\": " + orderAmount + "\n" +
                    "      }");
            if (i < tradeProvinceOrderAmountList.size() - 1) {
                mapData.append(",");
            } else {
                mapData.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"mapData\": " + mapData + ",\n" +
                "    \"valueName\": \"订单金额\"\n" +
                "  }\n" +
                "}";
    }
}
