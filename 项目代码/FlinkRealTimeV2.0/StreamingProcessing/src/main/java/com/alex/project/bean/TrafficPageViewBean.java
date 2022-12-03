package com.alex.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // app 版本号
    String vc;
    // 渠道
    String ch;
    // 地区
    String ar;
    // 新老访客状态标记
    String isNew;
    // 独立访客数
    Long uvCt;
    // 会话数
    Long svCt;
    // 页面浏览数
    Long pvCt;
    // 累计访问时长
    Long durSum;
    // 跳出会话数
    Long ujCt;
    // 时间戳
    Long ts;
}