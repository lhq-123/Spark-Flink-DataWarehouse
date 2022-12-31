package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserChangeCtPerType {
    // 变动类型
    String type;
    // 用户数
    Integer userCt;
}
