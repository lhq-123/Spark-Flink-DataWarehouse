#!/bin/bash

# 定义变量方便修改
APP=gmall
DB_LOCATION=hdfs://Flink01:8020/origin_data/gmall;
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else
   do_date=`date -d "-1 day" +%F`
fi

echo "================== 将日期为 $do_date 的用户日志数据导入到对应路径下====================="
# hadoop fs （hdfs dfs）
# hadoop fs -rm -r ${dir_a} && hadoop fs -cp ${dir_b} ${dir_a}
# hdfs dfs -mv $DB_LOCATION/log/app_log/$do_date $DB_LOCATION/ods/ods_log_inc/
hdfs dfs -rm -r hdfs://Flink01:8020/origin_data/gmall/ods/ods_log_inc/ && hadoop fs -cp hdfs://Flink01:8020/origin_data/gmall/log/app_log/2022-12-04 hdfs://Flink01:8020/origin_data/gmall/ods/ods_log_inc/
