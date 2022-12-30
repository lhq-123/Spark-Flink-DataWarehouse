package com.alex.mock.db.service.impl;

import com.  alex.mock.db.bean.OrderInfo;
import com.  alex.mock.db.bean.PaymentInfo;
import com.  alex.mock.db.constant.GmallConstant;
import com.  alex.mock.db.mapper.PaymentInfoMapper;
import com.  alex.mock.db.service.CouponUseService;
import com.  alex.mock.db.service.OrderInfoService;
import com.  alex.mock.db.service.OrderStatusLogService;
import com.  alex.mock.db.service.PaymentInfoService;
import com.  alex.mock.db.util.ParamUtil;
import com.  alex.mock.db.util.RanOpt;
import com.  alex.mock.db.util.RandomNumString;
import com.  alex.mock.db.util.RandomOptionGroup;
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

import static com.  alex.mock.db.constant.GmallConstant.*;

/**
 * <p>
 * 支付流水表 服务实现类
 * </p>
 *
 * @author    alex
 * @since 2020-02-24
 */
@Service
@Slf4j
public class PaymentInfoServiceImpl extends ServiceImpl<PaymentInfoMapper, PaymentInfo> implements PaymentInfoService {



    @Autowired
    OrderInfoService orderInfoService;

    @Autowired
    OrderStatusLogService orderStatusLogService;

    @Autowired
    CouponUseService couponUseService;


    @Value("${mock.date}")
    String mockDate;

    @Value("${mock.payment.rate:70}")
    String ifPaymentRate;

    @Value("${mock.payment.payment-type:30:60:10}")
    String paymentTypeRate;



    public void  genPayments(Boolean ifClear){
        Date date = ParamUtil.checkDate(mockDate);
        Integer ifPaymentWeight = ParamUtil.checkRatioNum(this.ifPaymentRate);
        Integer[] paymentTypeRateWeight = ParamUtil.checkRate(this.paymentTypeRate,3);

        RandomOptionGroup<Boolean> ifPayment=new RandomOptionGroup(new RanOpt(true,ifPaymentWeight),new RanOpt(false,100-ifPaymentWeight));
        RandomOptionGroup<String>  paymentOptionGroup=new RandomOptionGroup(new RanOpt(PAYMENT_TYPE_ALIPAY,paymentTypeRateWeight[0]),new RanOpt(PAYMENT_TYPE_WECHAT,paymentTypeRateWeight[1]),new RanOpt(PAYMENT_TYPE_UNION,paymentTypeRateWeight[2]));


        if(ifClear){
            remove(new QueryWrapper<PaymentInfo>());
        }


        QueryWrapper<OrderInfo> orderInfoQueryWrapper = new QueryWrapper<>();
        orderInfoQueryWrapper.eq("order_status",GmallConstant.ORDER_STATUS_UNPAID);
        orderInfoQueryWrapper.orderByAsc("id");
        List<OrderInfo> orderInfoList = orderInfoService.listWithDetail(orderInfoQueryWrapper);
        List<PaymentInfo> paymentList=new ArrayList();

        if(orderInfoList.size()==0){
            System.out.println("没有需要支付的订单！！ ");
            return;
        }
        for (OrderInfo orderInfo : orderInfoList) {
           if( ifPayment.getRandBoolValue() ){
               PaymentInfo paymentInfo = new PaymentInfo();
               paymentInfo.setOrderId(orderInfo.getId());
               paymentInfo.setTotalAmount(orderInfo.getFinalTotalAmount());
               paymentInfo.setUserId(orderInfo.getUserId());
               paymentInfo.setOutTradeNo(orderInfo.getOutTradeNo());
               paymentInfo.setAlipayTradeNo(RandomNumString.getRandNumString(1,9,34,""));
               paymentInfo.setPaymentType(paymentOptionGroup.getRandStringValue());
               paymentInfo.setSubject(orderInfo.getTradeBody());
               paymentInfo.setPaymentTime(date);
               paymentList.add(paymentInfo);

               orderInfo.setOrderStatus(GmallConstant.ORDER_STATUS_PAID);
           }
        }

         couponUseService.usedCoupon(orderInfoList);

         orderInfoService.updateOrderStatus(orderInfoList);
        log.warn("共有"+paymentList.size()+"订单完成支付");
         saveBatch(paymentList,100);



    }
}
