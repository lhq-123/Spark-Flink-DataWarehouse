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
        #检查Zookeeper状态
        zk.sh status
        sleep 3
        #启动 Kafka采集集群
        kfk.sh start
        sleep 3
        #各节点进程情况
        xcall.sh jps

        };;
"stop"){
        echo ================== 停止 集群 ==================

        #停止 Kafka采集集群
        kfk.sh stop
        sleep 3
        #停止 Zookeeper集群
        zk.sh stop
        #停止 Hadoop集群
        hdp.sh stop
        sleep 3
        #检查Zookeeper状态
        zk.sh status
        sleep 3
        #各节点进程情况
        xcall.sh jps
      
};;
esac
