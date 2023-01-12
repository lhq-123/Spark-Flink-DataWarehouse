-- 用户路径分析(用户在APP或网站中的访问路径),为了衡量网站优化的效果或营销推广的效果，以及了解用户行为偏好，时常要对访问路径进行分析,用户访问路径的可视化通常使用桑基图

CREATE DATABASE IF NOT EXISTS ads location 'hdfs://Flink01:8020/spark/gmall/ads';
use ads;
--建表语句
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ads/ads_page_path/';
--数据装载
insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2022-12-04' dt,
    recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
    (
        select
            recent_days,
            concat('step-',rn,':',page_id) source,
            concat('step-',rn+1,':',next_page_id) target
        from
            (
                select
                    recent_days,
                    page_id,
                    lead(page_id,1,null) over(partition by session_id,recent_days) next_page_id,
                        row_number() over (partition by session_id,recent_days order by view_time) rn
                from dwd.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
                where dt>=date_add('2022-12-04',-recent_days+1)
            )t1
    )t2
group by recent_days,source,target;