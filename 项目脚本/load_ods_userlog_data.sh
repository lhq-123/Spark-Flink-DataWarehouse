#!/bin/bash
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else
   do_date=`date -d "-1 day" +%F`
fi

echo "================== 将日期为 $do_date 的用户日志数据Load到对应表里====================="
# hadoop fs （hdfs dfs）
# hadoop fs -rm -r ${dir_a} && hadoop fs -cp ${dir_b} ${dir_a}
# hdfs dfs -mv $DB_LOCATION/log/app_log/$do_date $DB_LOCATION/ods/ods_log_inc/
sql="
load data inpath 'hdfs://Flink01:8020/origin_data/gmall/log/app_log/$do_date' into table ods.ods_log_inc partition(dt='$do_date');
"
hive -e "$sql"
