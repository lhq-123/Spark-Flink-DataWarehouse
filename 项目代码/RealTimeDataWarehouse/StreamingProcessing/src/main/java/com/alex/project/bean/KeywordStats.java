package com.alex.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 关键词来源   ---  辅助字段,不需要写入ClickHouse
    //@TransientSink
    private String source;
    // 关键词
    private String keyword;
    // 关键词出现频次
    private Long keyword_count;
    // 时间戳
    private Long ts;
}