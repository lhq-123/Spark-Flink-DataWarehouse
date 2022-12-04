#!/bin/bash

case $1 in
"start"){
        echo ================== 启动 集群 ==================

        #启动 Hadoop集群
        hdp.sh start
        sleep 3
        #启动 Zookeeper集群
        zk.sh start
        sleep 3
        #启动 Kafka采集集群
        kfk.sh start
        sleep 3
        #启动采集 Flume
        flume_applog_kafka.sh start
        sleep 3
        #启动日志消费 Flume
        flume_applog_hdfs.sh start
        sleep 3
        #启动业务消费 Flume
        flume_dblog_hdfs.sh start
        sleep 3
        #启动 maxwell
        mxw.sh start

        };;
"stop"){
        echo ================== 停止 集群 ==================

        #停止 Maxwell
        mxw.sh stop
        sleep 3
        #停止 业务消费Flume
        flume_dblog_hdfs.sh stop
        sleep 3
        #停止 日志消费Flume
        flume_applog_hdfs.sh stop
        sleep 3
        #停止 日志采集Flume
        flume_applog_kafka.sh stop
        sleep 3
        #停止 Kafka集群
        kfk.sh stop
        sleep 3
        #停止 Hadoop集群
        hdp.sh stop
        sleep 3
        #停止 Zookeeper集群
        zk.sh stop

};;
esac
