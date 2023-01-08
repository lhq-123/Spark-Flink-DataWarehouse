#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# Author: Alex_Liu
# Program function:


# 开启动态加载分区
# .config("hive.exec.dynamic.partition.mode", "nonstrict") \
#  .config("hive.exec.dynamic.partition", "true") \
#  限制success文件的生成
# .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('ods') \
    .master('yarn') \
    .config("spark.sql.crossJoin.enable", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.sql.broadcastTimeout", 3000) \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")
sc.setSystemProperty("HADOOP_USER_NAME", "root")


def hql_wrapper(sql_path):
    with open(sql_path, "r+", encoding="UTF-8") as f:
        for i in f.read().split(";"):
            if i.strip() != '':
                try:
                    spark.sql(i).show()
                    print(f'{i}\r\n 完成')
                except Exception as e:
                    print(f'{i}\r\n 出现了问题：{e}')
                    exit(1)


if __name__ == '__main__':
    hql_wrapper("/opt/module/alex/com/alex/mall/ods/hqlScript/test.hql")
