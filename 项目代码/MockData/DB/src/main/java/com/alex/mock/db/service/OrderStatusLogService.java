package com.alex.mock.db.service;

import com.  alex.mock.db.bean.OrderInfo;
import com.  alex.mock.db.bean.OrderStatusLog;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author    alex
 * @since 2020-02-24
 */
public interface OrderStatusLogService extends IService<OrderStatusLog> {
    public void  genOrderStatusLog(List<OrderInfo> orderInfoList);

}
