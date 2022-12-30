package com.alex.mock.db.service;

import com.  alex.mock.db.bean.OrderInfo;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 订单表 订单表 服务类
 * </p>
 *
 * @author    alex
 * @since 2020-02-23
 */
public interface OrderInfoService extends IService<OrderInfo> {
    public void genOrderInfos(boolean ifClear);

    public void updateOrderStatus(List<OrderInfo> orderInfoList);

    public List<OrderInfo> listWithDetail(Wrapper<OrderInfo> queryWrapper);

    public List<OrderInfo> listWithDetail(Wrapper<OrderInfo> queryWrapper, Boolean withSkuInfo);


}
