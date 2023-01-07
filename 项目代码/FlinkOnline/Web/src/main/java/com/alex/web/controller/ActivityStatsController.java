package com.alex.web.controller;

import com.alex.web.bean.ActivityReduceStats;
import com.alex.web.service.ActivityReduceService;
import com.alex.web.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/alex/realtime/activity")
public class ActivityStatsController {

    @Autowired
    private ActivityReduceService activityReduceService;

    @RequestMapping("/stats")
    public String getActivityStats(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<ActivityReduceStats> activityStatsList = activityReduceService.getActivityStats(date);
        if (activityStatsList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < activityStatsList.size(); i++) {
            ActivityReduceStats activityReduceStats = activityStatsList.get(i);
            Double activityReduceAmount = activityReduceStats.getActivityReduceAmount();
            Double originTotalAmount = activityReduceStats.getOriginTotalAmount();
            Double activitySubsidyRate = activityReduceStats.getActivitySubsidyRate();
            rows.append("{\n" +
                    "    \t\"activityReduceAmount\": " + activityReduceAmount + ",\n" +
                    "    \t\"originTotalAmount\": " + originTotalAmount + ",\n" +
                    "    \t\"activitySubsidyRate\": " + activitySubsidyRate + "\n" +
                    "    }");
            if (i < activityStatsList.size() - 1) {
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
                "        \"name\": \"活动减免金额\",\n" +
                "        \"id\": \"activityReduceAmount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"原始金额总和\",\n" +
                "        \"id\": \"originTotalAmount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"活动补贴率\",\n" +
                "        \"id\": \"activitySubsidyRate\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
