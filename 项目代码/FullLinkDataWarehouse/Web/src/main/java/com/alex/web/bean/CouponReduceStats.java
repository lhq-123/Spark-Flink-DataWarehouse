package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CouponReduceStats {
    // 优惠券减免金额
    Double couponReduceAmount;
    // 原始金额
    Double originTotalAmount;
    // 优惠券补贴率
    Double couponSubsidyRate;
}
