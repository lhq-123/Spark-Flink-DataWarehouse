package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserPageCt {
    // 页面 id
    String pageId;
    //独立访客数
    Integer uvCt;
}
