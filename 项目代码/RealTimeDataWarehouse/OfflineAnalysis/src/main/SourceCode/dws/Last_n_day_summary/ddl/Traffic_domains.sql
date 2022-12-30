--最近n日流量域汇总
CREATE DATABASE IF NOT EXISTS dws location 'hdfs://Flink01:8020/spark/gmall/dws';
USE dws;
--流量域访客页面粒度页面浏览最近n日汇总表
--建表语句
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_nd;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_nd
(
    `mid_id`          STRING COMMENT '访客id',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         STRING COMMENT '页面id',
    `during_time_7d`  BIGINT COMMENT '最近7日浏览时长',
    `view_count_7d`   BIGINT COMMENT '最近7日访问次数',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://Flink01:8020/spark/gmall/dws/dws_traffic_page_visitor_page_view_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
--数据装载
insert overwrite table dws_traffic_page_visitor_page_view_nd partition(dt='2022-12-04')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(if(dt>=date_add('2022-12-04',-6),during_time_1d,0)),
    sum(if(dt>=date_add('2022-12-04',-6),view_count_1d,0)),
    sum(during_time_1d),
    sum(view_count_1d)
from dws_traffic_page_visitor_page_view_1d
where dt>=date_add('2022-12-04',-29)
  and dt<='2022-12-04'
group by mid_id,brand,model,operate_system,page_id;