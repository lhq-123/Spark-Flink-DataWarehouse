package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.UserInfo;
import com.  alex.mock.db.mapper.UserInfoMapper;
import com.  alex.mock.db.service.UserInfoService;
import com.  alex.mock.db.util.*;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 用户表 服务实现类
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
@Service
@Slf4j
public class UserInfoServiceImpl extends ServiceImpl<UserInfoMapper, UserInfo> implements UserInfoService {

    @Autowired
    UserInfoMapper userInfoMapper;


    @Value("${mock.user.count}")
    String  userCountString;

    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.user.male-rate:50}")
    String maleRate;


    @Value("${mock.user.update-rate:20}")
    String updateRate;

    public void  genUserInfos( Boolean ifClear){

        Integer count = ParamUtil.checkCount(userCountString);
        Date date = ParamUtil.checkDate(mockDate);

        List<UserInfo>  userInfoList=new ArrayList<>();
        if(ifClear){
            userInfoMapper.truncateUserInfo();
        }else{
            updateUsers(  date);
        }

        for (int i = 0; i < count; i++) {
            userInfoList.add( initUserInfo(date)) ;
        }
        saveBatch(userInfoList,1000);
        log.warn("共生成{}名用户",userInfoList.size());




    }

    public UserInfo initUserInfo(Date date){
        Integer maleRateWeight = ParamUtil.checkRatioNum(this.maleRate);

        UserInfo userInfo = new UserInfo();
        String email = RandomEmail.getEmail(6, 12);
        String loginName = email.split("@")[0];
        userInfo.setLoginName(loginName);
        userInfo.setEmail(email);
        userInfo.setGender( new RandomOptionGroup(new RanOpt("M",maleRateWeight),new RanOpt("F",100-maleRateWeight)).getRandStringValue());
        String lastName = RandomName.insideLastName(userInfo.getGender());
        userInfo.setName(RandomName.getChineseFamilyName()+lastName);
        userInfo.setNickName(RandomName.getNickName(userInfo.getGender(),lastName));
        userInfo.setBirthday(DateUtils.addMonths(date, -1*RandomNum.getRandInt(15,55)*12));

        userInfo.setCreateTime(date);
        userInfo.setUserLevel(new RandomOptionGroup(new RanOpt("1",7),new RanOpt("2",2),new RanOpt("3",1)).getRandStringValue());
        userInfo.setPhoneNum("13"+RandomNumString.getRandNumString(1,9,9,""));
        return userInfo;
    }

    public void  updateUsers(Date date){


        Integer updateRateWeight = ParamUtil.checkRatioNum(this.updateRate);
        if (updateRateWeight==0){
            return ;
        }

        int count = count(new QueryWrapper<UserInfo>());

        String userIds = RandomNumString.getRandNumString(1, count, count *  updateRateWeight/ 100, ",",false);

        List<UserInfo> userInfoList = list(new QueryWrapper<UserInfo>().inSql("id", userIds));
        for (UserInfo userInfo : userInfoList) {
            int randInt = RandomNum.getRandInt(2, 7);
            if (randInt%2==0){
                String lastName = RandomName.insideLastName(userInfo.getGender());
                userInfo.setNickName(RandomName.getNickName(userInfo.getGender(),lastName));
            }
            if(randInt%3==0){
                userInfo.setUserLevel(new RandomOptionGroup(new RanOpt("1",7),new RanOpt("2",2),new RanOpt("3",1)).getRandStringValue());
            }
            if(randInt%5==0){
                String email = RandomEmail.getEmail(6, 12);
                userInfo.setEmail(email);
            }
            if(randInt%7==0){
                userInfo.setPhoneNum("13"+RandomNumString.getRandNumString(1,9,9,""));
            }
            userInfo.setOperateTime(date);
        }
        log.warn("共有{}名用户发生变更",userInfoList.size());
        saveOrUpdateBatch(userInfoList);



    }

}
