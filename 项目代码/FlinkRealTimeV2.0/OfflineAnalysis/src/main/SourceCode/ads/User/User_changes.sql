--用户变动统计(流失用户数和回流用户数)
--统计周期	指标
--最近1日	流失用户数	->之前活跃过的用户，最近一段时间未活跃，就称为流失用户。此处要求统计7日前（只包含7日前当天）活跃，但最近7日未活跃的用户总数。
--最近1日	回流用户数	->之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_user_change/';
--数据装载
insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
    (
        select
            '2022-12-04' dt,
            count(*) user_churn_count
        from dws.dws_user_user_login_td
        where dt='2022-12-04'
          and login_date_last=date_add('2022-12-04',-7)
    )churn
        join
    (
        select
            '2022-12-04' dt,
            count(*) user_back_count
        from
            (
                select
                    user_id,
                    login_date_last
                from dws.dws_user_user_login_td
                where dt='2022-12-04'
            )t1
                join
            (
                select
                    user_id,
                    login_date_last login_date_previous
                from dws.dws_user_user_login_td
                where dt=date_add('2022-12-04',-1)
            )t2
            on t1.user_id=t2.user_id
        where datediff(login_date_last,login_date_previous)>=8
    )back
    on churn.dt=back.dt;