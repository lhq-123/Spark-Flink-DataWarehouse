--用户新增活跃统计
--统计周期	    指标
--最近1/7/30日	新增用户数
--最近1/7/30日	活跃用户数

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_user_stats/';
--数据装载
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select
    '2022-12-04' dt,
    t1.recent_days,
    new_user_count,
    active_user_count
from
    (
        select
            recent_days,
            sum(if(login_date_last>=date_add('2022-12-04',-recent_days+1),1,0)) new_user_count
        from dws.dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
        where dt='2022-12-04'
        group by recent_days
    )t1
        join
    (
        select
            recent_days,
            sum(if(date_id>=date_add('2022-12-04',-recent_days+1),1,0)) active_user_count
        from dwd.dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days
        group by recent_days
    )t2
    on t1.recent_days=t2.recent_days;