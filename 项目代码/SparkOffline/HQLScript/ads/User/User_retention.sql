--用户留存率(新增留存和活跃留存)新增留存分析是分析某天的新增用户中,有多少人有后续的活跃行为.活跃留存分析是分析某天的活跃用户中,有多少人有后续的活跃行为.
--例如：2022-12-04新增100个用户,1日之后（2022-12-05）这100人中有80个人活跃了,那2022-12-04的1日留存数则为80,2022-12-04的1日留存率则为80%.
--统计每天的1至7日留存率

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
USE ads;
--建表语句
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_user_retention/';
--数据装载
insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2022-12-04' dt,
    login_date_first create_date,
    datediff('2022-12-04',login_date_first) retention_day,
    sum(if(login_date_last='2022-12-04',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='2022-12-04',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
    (
        select
            user_id,
            date_id login_date_first
        from dwd.dwd_user_register_inc
        where dt>=date_add('2022-12-04',-7)
          and dt<'2022-12-04'
    )t1
        join
    (
        select
            user_id,
            login_date_last
        from dws.dws_user_user_login_td
        where dt='2022-12-04'
    )t2
    on t1.user_id=t2.user_id
group by login_date_first;