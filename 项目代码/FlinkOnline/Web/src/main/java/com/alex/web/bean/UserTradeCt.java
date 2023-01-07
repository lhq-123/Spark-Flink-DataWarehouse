package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserTradeCt {
    // 交易类型
    String type;
    // 用户数
    Integer userCt;
}
