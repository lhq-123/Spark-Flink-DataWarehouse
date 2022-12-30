package com.alex.mock.db.service;

import com.  alex.mock.db.bean.PaymentInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 支付流水表 服务类
 * </p>
 *
 * @author    alex
 * @since 2020-02-24
 */
public interface PaymentInfoService extends IService<PaymentInfo> {
    public void  genPayments(Boolean ifClear);
}
