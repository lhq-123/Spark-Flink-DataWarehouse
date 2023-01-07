package com.alex.web.controller;

import com.alex.web.bean.CouponReduceStats;
import com.alex.web.service.CouponStatsService;
import com.alex.web.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/alex/realtime/coupon")
public class CouponStatsController {

    @Autowired
    private CouponStatsService couponStatsService;

    @RequestMapping("/stats")
    public String getCouponStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date){
        if(date == 1) {
            date = DateUtil.now();
        }
        List<CouponReduceStats> couponStatsList = couponStatsService.getCouponStats(date);
        if(couponStatsList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < couponStatsList.size(); i++) {
            CouponReduceStats couponReduceStats = couponStatsList.get(i);
            Double couponReduceAmount = couponReduceStats.getCouponReduceAmount();
            Double originTotalAmount = couponReduceStats.getOriginTotalAmount();
            Double couponSubsidyRate = couponReduceStats.getCouponSubsidyRate();
            rows.append("{\n" +
                    "        \"couponReduceAmount\": "+ couponReduceAmount +",\n" +
                    "        \"originTotalAmount\": "+ originTotalAmount +",\n" +
                    "        \"couponSubsidyRate\": "+ couponSubsidyRate +"\n" +
                    "      }");
            if(i < couponStatsList.size() - 1) {
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
                "        \"name\": \"优惠券减免金额\",\n" +
                "        \"id\": \"couponReduceAmount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"原始金额总和\",\n" +
                "        \"id\": \"originTotalAmount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"优惠券补贴率\",\n" +
                "        \"id\": \"couponSubsidyRate\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": "+ rows +"\n" +
                "  }\n" +
                "}";
    }
}
