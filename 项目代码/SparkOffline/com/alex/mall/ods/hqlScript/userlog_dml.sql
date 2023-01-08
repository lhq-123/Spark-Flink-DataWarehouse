--如果使用SparkSQL的话是可以使用set在SQL脚本里传参数的，但是在hive on spark里是不支持的
--SET DB_LOCATION=hdfs://Flink01:8020/spark/gmall;
CREATE DATABASE IF NOT EXISTS ods location 'hdfs://Flink01:8020/spark/gmall/ods';
USE ods;
--增量日志表
DROP TABLE IF EXISTS ods_log_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_log_inc
(
    `common`   STRUCT<ar :STRING,ba :STRING,ch :STRING,is_new :STRING,md :STRING,mid :STRING,os :STRING,uid :STRING,vc:STRING> COMMENT '公共信息',
    `page`     STRUCT<during_time :STRING,item :STRING,item_type :STRING,last_page_id :STRING,page_id:STRING,source_type :STRING> COMMENT '页面信息',
    `actions`  ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id:STRING>> COMMENT '曝光信息',
    `start`    STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms:BIGINT> COMMENT '启动信息',
    `err`      STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息',
    `ts`       BIGINT  COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION 'hdfs://Flink01:8020/spark/gmall/ods/ods_log_inc/';
