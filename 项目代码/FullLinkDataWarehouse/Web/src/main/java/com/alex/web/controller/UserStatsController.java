package com.alex.web.controller;

import com.alex.web.bean.UserChangeCtPerType;
import com.alex.web.bean.UserPageCt;
import com.alex.web.bean.UserTradeCt;
import com.alex.web.service.UserStatsService;
import com.alex.web.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/alex/realtime/user")
public class UserStatsController {

    @Autowired
    private UserStatsService userStatsService;

    @RequestMapping("/uvPerPage")
    public String getUvPerPage(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<UserPageCt> uvByPageList = userStatsService.getUvByPage(date);
        if (uvByPageList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < uvByPageList.size(); i++) {
            UserPageCt userPageCt = uvByPageList.get(i);
            String pageId = userPageCt.getPageId();
            Integer uvCt = userPageCt.getUvCt();
            rows.append("{\n" +
                    "        \"page_id\": \"" + pageId + "\",\n" +
                    "        \"uv_ct\": \"" + uvCt + "\"\n" +
                    "      }");
            if (i < uvByPageList.size() - 1) {
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
                "        \"name\": \"页面\",\n" +
                "        \"id\": \"page_id\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"id\": \"uv_ct\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/userChangeCt")
    public String getUserChange(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<UserChangeCtPerType> userChangeCtList = userStatsService.getUserChangeCt(date);
        if (userChangeCtList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < userChangeCtList.size(); i++) {
            UserChangeCtPerType userChangeCt = userChangeCtList.get(i);
            String type = userChangeCt.getType();
            Integer userCt = userChangeCt.getUserCt();
            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"user_ct\": \"" + userCt + "\"\n" +
                    "}");
            if (i < userChangeCtList.size() - 1) {
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
                "        \"name\": \"变动类型\",\n" +
                "        \"id\": \"type\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"用户数\",\n" +
                "        \"id\": \"user_ct\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/userTradeCt")
    public String getUserTradeCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<UserTradeCt> tradeUserCtList = userStatsService.getTradeUserCt(date);
        if (tradeUserCtList == null) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < tradeUserCtList.size(); i++) {
            UserTradeCt userTradeCt = tradeUserCtList.get(i);
            String type = userTradeCt.getType();
            Integer userCt = userTradeCt.getUserCt();
            rows.append("{\n" +
                    "\t\"type\": \"" + type + "\",\n" +
                    "\t\"user_ct\": \"" + userCt + "\"\n" +
                    "}");
            if (i < tradeUserCtList.size() - 1) {
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
                "        \"name\": \"交易类型\",\n" +
                "        \"id\": \"type\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"新增用户数\",\n" +
                "        \"id\": \"user_ct\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
