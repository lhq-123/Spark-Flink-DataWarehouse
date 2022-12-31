package com.alex.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;

@Data
@AllArgsConstructor
public class TrafficVisitorTypeStats {
    // 新老访客状态标记
    String isNew;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 会话数
    Long svCt;
    // 跳出会话数
    Long ujCt;
    // 累计访问时长
    Long durSum;
    // 跳出率
    public Double getUjRate(){
        if(svCt == 0) {
            return 0.0;
        }
        return (double)ujCt/(double)svCt;
    }
    // 会话平均在线时长（秒）
    public Double getAvgDurSum() {
        if(svCt == 0) {
            return 0.0;
        }
        return (double)durSum/(double) svCt / 1000;
    }
    // 会话平均访问页面数
    public Double getAvgPvCt(){
        if(svCt == 0) {
            return 0.0;
        }
        return (double)pvCt / (double) svCt;
    }


}
