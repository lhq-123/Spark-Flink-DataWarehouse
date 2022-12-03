package com.alex.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Alex_liu
 * @create 2022-12-01 16:47
 * @Description 订单实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;

}
