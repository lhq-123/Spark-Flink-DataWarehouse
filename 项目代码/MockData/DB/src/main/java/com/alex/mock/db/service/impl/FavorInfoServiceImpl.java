package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.FavorInfo;
import com.  alex.mock.db.mapper.FavorInfoMapper;
import com.  alex.mock.db.mapper.SkuInfoMapper;
import com.  alex.mock.db.mapper.UserInfoMapper;
import com.  alex.mock.db.service.FavorInfoService;
import com.  alex.mock.db.util.ParamUtil;
import com.  alex.mock.db.util.RanOpt;
import com.  alex.mock.db.util.RandomNum;
import com.  alex.mock.db.util.RandomOptionGroup;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 商品收藏表 服务实现类
 * </p>
 *
 * @author  alex
 * @since 2020-02-24
 */
@Service
@Slf4j
public class FavorInfoServiceImpl extends ServiceImpl<FavorInfoMapper, FavorInfo> implements FavorInfoService {

    @Autowired
    SkuInfoMapper skuInfoMapper;

    @Autowired
    UserInfoMapper userInfoMapper;



    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.favor.count}")
    String  countString;

    @Value("${mock.favor.cancel-rate:50}")
    String cancelRate;

    public  void genFavors( Boolean ifClear){
        Integer count = ParamUtil.checkCount(countString);

        if(ifClear){
            remove(new QueryWrapper<>());
        }
        Integer skuTotal = skuInfoMapper.selectCount(new QueryWrapper<>());
        Integer userTotal = userInfoMapper.selectCount(new QueryWrapper<>());

        List<FavorInfo> favorInfoList= new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Long userId = RandomNum.getRandInt(1, userTotal)+0L;
            Long skuId = RandomNum.getRandInt(1, skuTotal)+0L;
            favorInfoList.add(initFavorInfo(skuId,  userId)) ;
        }
        saveBatch(favorInfoList,100);
        log.warn("共生成收藏"+favorInfoList.size()+"条");
    }


    public  FavorInfo initFavorInfo( Long skuId,Long userId  ){

        Date date = ParamUtil.checkDate(mockDate);
        Integer cancelRateWeight = ParamUtil.checkRatioNum(this.cancelRate);

        RandomOptionGroup<String> isCancelOptionGroup=new RandomOptionGroup(new RanOpt("1",cancelRateWeight),new RanOpt("0",100-cancelRateWeight) );

        FavorInfo favorInfo = new FavorInfo();
        favorInfo.setSkuId(skuId);
        String isCancel = isCancelOptionGroup.getRandStringValue();
        favorInfo.setIsCancel (isCancel);
        favorInfo.setUserId(userId);
        favorInfo.setCreateTime(date);
        if (isCancel.equals("1")) {
            favorInfo.setCancelTime(date);
        }
        return favorInfo;
    }
}
