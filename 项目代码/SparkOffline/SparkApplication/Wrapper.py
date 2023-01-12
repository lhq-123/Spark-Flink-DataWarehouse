#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# Author: Alex_Liu
# Program function:  执行ODS层SQL脚本的python脚本
import logging
import os
import sys
import time

from pyspark.sql import SparkSession


path = os.path.dirname(os.path.dirname(__file__))  # /opt/module/alex/com/alex/mall
arg = sys.argv[1].split("~")
db = arg[0]
location = arg[1]


def hql_spark_wrapper(sql_path):
    spark = SparkSession.builder \
        .appName('ods_ddl') \
        .master('yarn') \
        .config("spark.sql.crossJoin.enable", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.broadcastTimeout", 3000) \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    sc.setSystemProperty("HADOOP_USER_NAME", "root")

    with open(sql_path, "r+", encoding="UTF-8") as f:
        for i in f.read().split(";"):
            if i.strip() != '':
                try:
                    spark.sql(i).show()
                except Exception as e:
                    logging.error(f'{i}\r\n 发生了异常：{e}')
                    exit(1)
        logging.info('>>>>>>>>>>>>>>>>>>>全量表入库完成<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        spark.stop()


def hql_mr_wrapper():
    try:
        os.system(f"""
    hive -hivevar DB="{db}" -hivevar DB_LOCATION="{location}/{db}" -f {path}/{db}/B_Incr.sql
    """)
        logging.info('>>>>>>>>>>>>>>>>>>>增量表入库完成<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    except Exception as e:
        logging.error(f' 增量表入库发生了异常：{e}')


if __name__ == '__main__':
    logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>构建ODS层<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    hql_spark_wrapper(f"{path}/ods/B_Full.sql")
    hql_mr_wrapper()
