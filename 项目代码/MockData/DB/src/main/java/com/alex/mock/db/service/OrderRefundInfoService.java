package com.alex.mock.db.service;

import com.  alex.mock.db.bean.OrderRefundInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 退单表 服务类
 * </p>
 *
 * @author  alex
 * @since 2020-02-25
 */
public interface OrderRefundInfoService extends IService<OrderRefundInfo> {

    public void  genRefundsOrFinish(Boolean ifClear);
}
