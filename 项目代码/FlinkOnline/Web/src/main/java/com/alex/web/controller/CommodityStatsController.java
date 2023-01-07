package com.alex.web.controller;

import com.alex.web.bean.CategoryCommodityStats;
import com.alex.web.bean.SpuCommodityStats;
import com.alex.web.bean.TrademarkCommodityStats;
import com.alex.web.bean.TrademarkOrderAmountPieGraph;
import com.alex.web.service.CommodityStatsService;
import com.alex.web.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/alex/realtime/commodity")
public class CommodityStatsController {

    @Autowired
    private CommodityStatsService commodityStatsService;

    @RequestMapping("/trademark")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        if (date == 0) {
            date = DateUtil.now();
        }

        Map gmvByTm = commodityStatsService.getGmvByTm(date, limit);

        Set tmNames = gmvByTm.keySet();
        Collection gmvs = gmvByTm.values();

        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(tmNames, "\",\"") +
                "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"商品品牌\"," +
                "        \"data\": [" +
                StringUtils.join(gmvs, ",") +
                "        ]" +
                "      }" +
                "    ]" +
                "  }" +
                "}";
    }

    @RequestMapping("/trademark2")
    public String getTrademarkCommodityStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrademarkCommodityStats> trademarkCommodityStatsList = commodityStatsService.getTrademarkCommodityStatsService(date);
        if (trademarkCommodityStatsList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < trademarkCommodityStatsList.size(); i++) {
            TrademarkCommodityStats trademarkCommodityStats = trademarkCommodityStatsList.get(i);
            String trademarkName = trademarkCommodityStats.getTrademarkName();
            Integer orderCt = trademarkCommodityStats.getOrderCt();
            Integer uuCt = trademarkCommodityStats.getUuCt();
            Double orderAmount = trademarkCommodityStats.getOrderAmount();
            Integer refundCt = trademarkCommodityStats.getRefundCt();
            Integer refundUuCt = trademarkCommodityStats.getRefundUuCt();

            rows.append("{\n" +
                    "\t\"trademark\": \"" + trademarkName + "\",\n" +
                    "\t\"order_count\": \"" + orderCt + "\",\n" +
                    "\t\"uu_count\": \"" + uuCt + "\",\n" +
                    "\t\"order_amount\": \"" + orderAmount + "\",\n" +
                    "\t\"refund_count\": \"" + refundCt + "\",\n" +
                    "\t\"refund_uu_count\": \"" + refundUuCt + "\"\n" +
                    "}");
            if (i < trademarkCommodityStatsList.size() - 1) {
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
                "        \"name\": \"品牌名称\",\n" +
                "        \"id\": \"trademark\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单数\",\n" +
                "        \"id\": \"order_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单人数\",\n" +
                "        \"id\": \"uu_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_amount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"退单数\",\n" +
                "        \"id\": \"refund_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"退单人数\",\n" +
                "        \"id\": \"refund_uu_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/tmPieGraph")
    public String getTmOrderAmtPieGra(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if(date == 1) {
            date = DateUtil.now();
        }
        List<TrademarkOrderAmountPieGraph> tmOrderAmtPieGraList = commodityStatsService.getTmOrderAmtPieGra(date);
        if(tmOrderAmtPieGraList == null || tmOrderAmtPieGraList.size() == 0) {
            return "";
        }
        StringBuilder dataSet = new StringBuilder("[");
        for (int i = 0; i < tmOrderAmtPieGraList.size(); i++) {
            TrademarkOrderAmountPieGraph trademarkOrderAmountPieGraph = tmOrderAmtPieGraList.get(i);
            String trademarkName = trademarkOrderAmountPieGraph.getTrademarkName();
            Double orderAmount = trademarkOrderAmountPieGraph.getOrderAmount();
            dataSet.append("{\n" +
                    "      \"name\": \""+ trademarkName +"\",\n" +
                    "      \"value\": "+ orderAmount +"\n" +
                    "    }");
            if(i < tmOrderAmtPieGraList.size() - 1) {
                dataSet.append(",");
            } else {
                dataSet.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": "+ dataSet +"\n" +
                "}";
    }

    @RequestMapping("/category")
    public String getCategoryStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<CategoryCommodityStats> categoryStatsServiceList = commodityStatsService.getCategoryStatsService(date);
        if (categoryStatsServiceList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < categoryStatsServiceList.size(); i++) {
            CategoryCommodityStats categoryCommodityStats = categoryStatsServiceList.get(i);
            String category1Name = categoryCommodityStats.getCategory1_name();
            String category2Name = categoryCommodityStats.getCategory2_name();
            String category3Name = categoryCommodityStats.getCategory3_name();
            Integer orderCt = categoryCommodityStats.getOrderCt();
            Integer uuCt = categoryCommodityStats.getUuCt();
            Double orderAmount = categoryCommodityStats.getOrderAmount();
            Integer refundCt = categoryCommodityStats.getRefundCt();
            Double refundUcCt = categoryCommodityStats.getRefundUcCt();

            rows.append("{\n" +
                    "\t\"category1_name\": \"" + category1Name + "\",\n" +
                    "\t\"category2_name\": \"" + category2Name + "\",\n" +
                    "\t\"category3_name\": \"" + category3Name + "\",\n" +
                    "\t\"order_count\": \"" + orderCt + "\",\n" +
                    "\t\"uu_count\": \"" + uuCt + "\",\n" +
                    "\t\"order_amount\": \"" + orderAmount + "\",\n" +
                    "\t\"refund_count\": \"" + refundCt + "\",\n" +
                    "\t\"refund_uu_count\": \"" + refundUcCt + "\"\n" +
                    "}");
            if (i < categoryStatsServiceList.size() - 1) {
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
                "        \"name\": \"一级品类名称\",\n" +
                "        \"id\": \"category1_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"二级品类名称\",\n" +
                "        \"id\": \"category2_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"三级品类名称\",\n" +
                "        \"id\": \"category3_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单数\",\n" +
                "        \"id\": \"order_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单人数\",\n" +
                "        \"id\": \"uu_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_amount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"退单数\",\n" +
                "        \"id\": \"refund_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"退单人数\",\n" +
                "        \"id\": \"refund_uu_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/spu")
    public String getSpuStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<SpuCommodityStats> spuCommodityStatsList = commodityStatsService.getSpuCommodityStats(date);
        if (spuCommodityStatsList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < spuCommodityStatsList.size(); i++) {
            SpuCommodityStats spuCommodityStats = spuCommodityStatsList.get(i);
            String spuName = spuCommodityStats.getSpuName();
            Integer orderCt = spuCommodityStats.getOrderCt();
            Integer uuCt = spuCommodityStats.getUuCt();
            Double orderAmount = spuCommodityStats.getOrderAmount();
            rows.append("{\n" +
                    "\t\"spu_name\": \"" + spuName + "\",\n" +
                    "\t\"order_count\": \"" + orderCt + "\",\n" +
                    "\t\"uu_count\": \"" + uuCt + "\",\n" +
                    "\t\"order_amount\": \"" + orderAmount + "\"\n" +
                    "}");
            if (i < spuCommodityStatsList.size() - 1) {
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
                "        \"name\": \"SPU 名称\",\n" +
                "        \"id\": \"spu_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"下单次数\",\n" +
                "        \"id\": \"order_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"下单人数\",\n" +
                "        \"id\": \"uu_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"订单金额\",\n" +
                "        \"id\": \"order_amount\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
