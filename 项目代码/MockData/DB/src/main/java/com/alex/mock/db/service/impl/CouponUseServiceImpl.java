package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.*;
import com.  alex.mock.db.constant.GmallConstant;
import com.  alex.mock.db.mapper.CouponUseMapper;
import com.  alex.mock.db.service.CouponInfoService;
import com.  alex.mock.db.service.CouponUseService;
import com.  alex.mock.db.service.SkuInfoService;
import com.  alex.mock.db.service.UserInfoService;
import com.  alex.mock.db.util.ParamUtil;
import com.  alex.mock.db.util.RandomNum;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 优惠券领用表 服务实现类
 * </p>
 *
 * @author  alex
 * @since 2020-02-26
 */
@Service
@Slf4j
public class CouponUseServiceImpl extends ServiceImpl<CouponUseMapper, CouponUse> implements CouponUseService {

    @Autowired
    CouponInfoService couponInfoService;

    @Autowired
    SkuInfoService skuInfoService;

    @Autowired
    UserInfoService userInfoService;

    @Autowired
    CouponUseService couponUseService;

    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.coupon.user-count:1000}")
    String userCount;


    public void genCoupon(  Boolean ifClear) {
        Date date = ParamUtil.checkDate(mockDate);
        Integer userCount = ParamUtil.checkCount(this.userCount);
        if (ifClear) {
            remove(new QueryWrapper<>());

        }
        QueryWrapper<UserInfo> userInfoQueryWrapper = new QueryWrapper<>();
        userInfoQueryWrapper.last("limit " + userCount);
        Integer userTotal = userInfoService.count(userInfoQueryWrapper);


        List<CouponInfo> couponInfoList = couponInfoService.list(new QueryWrapper<>());
          userCount = Math.min(userTotal, userCount);
        List<CouponUse> couponUseList = new ArrayList<>();
        for (CouponInfo couponInfo : couponInfoList) {
            for (int userId = 1; userId <= userCount; userId++) {
                CouponUse couponUse = new CouponUse();
                couponUse.setCouponStatus(GmallConstant.COUPON_STATUS_UNUSED);
                couponUse.setGetTime(date);
                couponUse.setExpireTime(couponInfo.getExpireTime());
                couponUse.setUserId(userId+0L);
                couponUse.setCouponId(couponInfo.getId());
                couponUseList.add(couponUse);
            }
        }
         log.warn("共优惠券"+couponUseList.size()+"张");
        saveBatch(couponUseList);


    }

    public List<CouponUse>  usingCoupon(List<OrderInfo> orderInfoList) {
        List<CouponUse> couponUseListForUpdate = new ArrayList<>();

        Date date = ParamUtil.checkDate(mockDate);

        for (OrderInfo orderInfo : orderInfoList) {
            boolean canUseCoupon = false;
            List<OrderDetail> orderDetailList = orderInfo.getOrderDetailList();
            List<CouponUse> couponUseList = couponUseService.list(new QueryWrapper<CouponUse>().eq("user_id", orderInfo.getUserId()));

            for (CouponUse couponUse : couponUseList) {
                CouponInfo couponInfo = couponInfoService.getById(couponUse.getCouponId());
                couponUse.setCouponInfo(couponInfo);
            }
            orderDetailLoop:
            for (OrderDetail orderDetail : orderDetailList) {
                SkuInfo skuInfo = skuInfoService.getById(orderDetail.getSkuId());
                orderDetail.setSkuInfo(skuInfo);


                for (CouponUse couponUse : couponUseList) {
                    if (!couponUse.getCouponStatus().equals(GmallConstant.COUPON_STATUS_UNUSED)) {
                        continue;
                    }
                    CouponInfo couponInfo = couponUse.getCouponInfo();
                    //核心判断是否能使用上购物券 spuid tmid cateid必须有一个满足
                    canUseCoupon=skuInfo.getSpuId().equals(couponInfo.getSpuId())
                            || skuInfo.getTmId().equals(couponInfo.getTmId())
                            ||skuInfo.getCategory3Id().equals(couponInfo.getCategory3Id());
                    if (canUseCoupon) {
                        couponUse.setOrderId(orderInfo.getId());
                        couponUse.setOrderInfo(orderInfo );
                        if (couponUse.getCouponStatus().equals(GmallConstant.COUPON_STATUS_UNUSED)) {
                            couponUse.setCouponStatus(GmallConstant.COUPON_STATUS_USING);
                            couponUse.setUsingTime(date);
                        }
                        couponUseListForUpdate.add(couponUse);
                        canUseCoupon = true;
                        break orderDetailLoop;
                    }
                }


            }

            if (canUseCoupon) {
                BigDecimal reduce = BigDecimal.valueOf(RandomNum.getRandInt(1, orderInfo.getOriginalTotalAmount().intValue() / 2));
                if (orderInfo.getBenefitReduceAmount() == null) {
                    orderInfo.setBenefitReduceAmount(reduce);
                } else {
                    orderInfo.setBenefitReduceAmount(orderInfo.getBenefitReduceAmount().add(reduce));
                }
                orderInfo.sumTotalAmount();
            }
        }

         return couponUseListForUpdate ;

    }

    public  void  saveCouponUseList( List<CouponUse> couponUseList){
        saveBatch(couponUseList,100);
    }


    public   void  usedCoupon(List<OrderInfo> orderInfoList){
        Date date = ParamUtil.checkDate(mockDate);
        List<Long>  orderIdList=new ArrayList<>();
        for (OrderInfo orderInfo : orderInfoList) {
            orderIdList.add(orderInfo.getId());
        }
        CouponUse couponUse = new CouponUse();
        couponUse.setUsedTime(date);
        couponUse.setCouponStatus(GmallConstant.COUPON_STATUS_USED);
        update(couponUse,new QueryWrapper<CouponUse>().in("order_id",orderIdList));


    }

}
