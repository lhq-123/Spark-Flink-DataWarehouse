package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.*;
import com.alex.mock.db.constant.Constant;
import com.  alex.mock.db.mapper.*;
import com.  alex.mock.db.service.*;
import com.  alex.mock.db.util.*;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 订单表 订单表 服务实现类
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
@Service
@Slf4j
public class OrderInfoServiceImpl extends ServiceImpl<OrderInfoMapper, OrderInfo> implements OrderInfoService {

    @Autowired
    OrderInfoMapper orderInfoMapper;

    @Autowired
    SkuInfoMapper skuInfoMapper;

    @Autowired
    UserInfoMapper userInfoMapper;

    @Autowired
    BaseProvinceMapper provinceMapper;

    @Autowired
    OrderDetailService orderDetailService;

    @Autowired
    OrderStatusLogService orderStatusLogService;

    @Autowired
    ActivityOrderService activityOrderService;

    @Autowired
    CouponUseServiceImpl couponUseService;

    @Autowired
    SkuInfoService skuInfoService;

    @Autowired
    CartInfoService cartInfoService;

    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.order.user-rate:50}")
    String orderUserRate;

    @Value("${mock.order.sku-rate:50}")
    String orderSkuRate;

    @Value("${mock.order.join-activity:0}")
    String joinActivity;

    @Value("${mock.order.use-coupon:0}")
    String useCoupon;


    public  OrderInfo initOrder(Long userId ,Integer  provinceTotal ,List<Long> cartIdListForUpdate ) {
        Date date = ParamUtil.checkDate(mockDate);

        Integer orderSkuRateWeight = ParamUtil.checkRatioNum(this.orderSkuRate);
        Integer orderUserWeight = ParamUtil.checkRatioNum(this.orderUserRate);

        RandomOptionGroup<Boolean>  isOrderUserOptionGroup=new RandomOptionGroup(orderUserWeight,100-orderUserWeight);

        RandomOptionGroup<Boolean>  isOrderSkuOptionGroup=new RandomOptionGroup(orderSkuRateWeight,100-orderSkuRateWeight);

        if(!isOrderUserOptionGroup.getRandBoolValue()) {
            return  null;
        }

        OrderInfo orderInfo =new OrderInfo();
        orderInfo.setUserId(userId);
        orderInfo.setConsignee(RandomName.genName());
        orderInfo.setConsigneeTel("13"+RandomNumString.getRandNumString(0,9,9,""));
        orderInfo.setCreateTime(date);
        orderInfo.setDeliveryAddress("第"+ RandomNum.getRandInt(1,20)+"大街第"+RandomNum.getRandInt(1,40)+"号楼"+RandomNum.getRandInt(1,9)+"单元"+RandomNumString.getRandNumString(1,9,3,"")+"门");
        orderInfo.setExpireTime(DateUtils.addMinutes(date,15));
        orderInfo.setImgUrl("http://img.gmall.com/"+RandomNumString.getRandNumString(1,9,6,"")+".jpg");
        orderInfo.setOrderStatus(Constant.ORDER_STATUS_UNPAID);
        orderInfo.setOrderComment("描述"+RandomNumString.getRandNumString(1,9,6,""));
        orderInfo.setOutTradeNo(RandomNumString.getRandNumString(1,9,15,""));
        orderInfo.setFeightFee(BigDecimal.valueOf(RandomNum.getRandInt(5,20)));
        int provinceId = RandomNum.getRandInt(1, provinceTotal);
        orderInfo.setProvinceId(provinceId);


        List<CartInfo> userCartList = cartInfoService.list(new QueryWrapper<CartInfo>().eq("user_id", userId));
        List<OrderDetail> orderDetailList=new ArrayList<>();
        for (CartInfo cartInfo : userCartList) {
            if(isOrderSkuOptionGroup.getRandBoolValue()){
                OrderDetail orderDetail =new OrderDetail();
                orderDetail.setImgUrl(cartInfo.getImgUrl());
                orderDetail.setSkuNum(cartInfo.getSkuNum());
                orderDetail.setSkuName(cartInfo.getSkuName());
                orderDetail.setSkuId(cartInfo.getSkuId());
                orderDetail.setOrderPrice(cartInfo.getCartPrice());
                orderDetail.setCreateTime(date);
                orderDetail.setSourceId(cartInfo.getSourceId());
                orderDetail.setSourceType(cartInfo.getSourceType());
                orderDetail.setCreateTime(date);
                orderDetailList.add(orderDetail);
                cartIdListForUpdate.add(cartInfo.getId());
            }
        }

        orderInfo.setOrderDetailList(orderDetailList);
        orderInfo.setUserId(userId+0L);
        orderInfo.sumTotalAmount();
        orderInfo.setTradeBody(orderInfo.getOrderSubject());


        return orderInfo;

    }

    @Transactional(rollbackFor = Exception.class)
    public void genOrderInfos(boolean ifClear ){
        Boolean joinActivity = ParamUtil.checkBoolean(this.joinActivity);
        Boolean useCoupon = ParamUtil.checkBoolean(this.useCoupon);

        if(useCoupon){
            couponUseService.genCoupon(ifClear);
        }

        if(ifClear){
                remove(new QueryWrapper<>());
                orderDetailService.remove(new QueryWrapper<>());
                orderStatusLogService.remove(new QueryWrapper<>());
            }
            List<CartInfo> cartInfoList = cartInfoService.list(new QueryWrapper<CartInfo>().select("distinct user_id").eq("is_ordered",0));
           Integer provinceTotal = provinceMapper.selectCount(new QueryWrapper<>());

           List<OrderInfo> orderInfoList= new ArrayList<>();
           List<Long>  cartIdListForUpdate=new ArrayList<>();
           for (CartInfo cartInfo : cartInfoList) {
               OrderInfo orderInfo = initOrder(cartInfo.getUserId(), provinceTotal,cartIdListForUpdate);
               if(orderInfo!=null&&orderInfo.getOrderDetailList()!=null&&orderInfo.getOrderDetailList().size()>0){
                   orderInfoList.add(orderInfo);
               }
          }
           CartInfo cartInfo=new CartInfo();
           cartInfo.setIsOrdered(1);
           cartInfo.setOrderTime(new Date());
           cartInfoService.update(cartInfo,new QueryWrapper<CartInfo>().in("id",cartIdListForUpdate));

           log.warn("共生成订单"+orderInfoList.size()+"条");
           List<ActivityOrder> activityOrderList=null;
            if(joinActivity){
                activityOrderList= activityOrderService.genActivityOrder(orderInfoList, ifClear);
            }

            List<CouponUse> couponUseList=null;
            if(useCoupon){
                couponUseList = couponUseService.usingCoupon(orderInfoList);
            }

           saveBatch(orderInfoList);
           if(activityOrderList!=null&&activityOrderList.size()>0){
               for (ActivityOrder activityOrder : activityOrderList) {
                   activityOrder.setOrderId ( activityOrder.getOrderInfo().getId());
               }
               activityOrderService.saveActivityOrderList(activityOrderList);
           }

            if(couponUseList!=null&&couponUseList.size()>0){
                for (CouponUse couponUse : couponUseList) {
                    couponUse.setOrderId ( couponUse.getOrderInfo().getId());
                }
                couponUseService.saveCouponUseList(couponUseList);
            }


           orderStatusLogService.genOrderStatusLog(orderInfoList);
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean saveBatch( List<OrderInfo> orderInfoList){
        super.saveBatch(orderInfoList, 100);
        List orderDetailAllList=new ArrayList();
        for (OrderInfo orderInfo : orderInfoList) {
            Long orderId = orderInfo.getId();
            List<OrderDetail> orderDetailList = orderInfo.getOrderDetailList();
            for (OrderDetail orderDetail : orderDetailList) {
                orderDetail.setOrderId(orderId);
                orderDetailAllList.add(orderDetail)  ;
            }
        }
        return orderDetailService.saveBatch(orderDetailAllList,100);


    }


    public void updateOrderStatus(List<OrderInfo> orderInfoList){
        Date date = ParamUtil.checkDate(mockDate);
        if(orderInfoList.size()==0){
            System.out.println("没有需要更新状态的订单！！ ");
            return;
        }
        List orderInfoUpdateList=new ArrayList();

        for (OrderInfo orderInfo : orderInfoList) {
            OrderInfo orderInfoForUpdate = new OrderInfo();
            orderInfoForUpdate.setId(orderInfo.getId());
            orderInfoForUpdate.setOrderStatus(orderInfo.getOrderStatus());
            orderInfoForUpdate.setOperateTime(date);
            orderInfoUpdateList.add(orderInfoForUpdate);
        }
        System.out.println("状态更新"+orderInfoUpdateList.size()+"个订单");
        orderStatusLogService.genOrderStatusLog(orderInfoUpdateList);


        saveOrUpdateBatch(orderInfoUpdateList,100);

    }

    public List<OrderInfo> listWithDetail(Wrapper<OrderInfo> queryWrapper ) {
       return  listWithDetail( queryWrapper ,false);
    }

    public List<OrderInfo> listWithDetail(Wrapper<OrderInfo> queryWrapper ,Boolean withSkuInfo){
        List<OrderInfo> orderInfoList = super.list(queryWrapper);
        for (OrderInfo orderInfo : orderInfoList) {
            List<OrderDetail> orderDetailList = orderDetailService.list(new QueryWrapper<OrderDetail>().eq("order_id", orderInfo.getId()));
            if(withSkuInfo){
                for (OrderDetail orderDetail : orderDetailList) {
                    SkuInfo skuInfo = skuInfoService.getById(orderDetail.getSkuId());
                    orderDetail.setSkuInfo(skuInfo);
                }
            }
            orderInfo.setOrderDetailList(orderDetailList);
        }
        return orderInfoList;


    }




}
