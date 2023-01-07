package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ActivityReduceStats {
    // 活动减免金额
    Double activityReduceAmount;
    // 原始金额
    Double originTotalAmount;
    // 活动补贴率
    Double activitySubsidyRate;
}
