package com.alex.web.service;

import com.alex.web.bean.UserChangeCtPerType;
import com.alex.web.bean.UserPageCt;
import com.alex.web.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserPageCt> getUvByPage(Integer date);

    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}
