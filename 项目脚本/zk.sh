#!/bin/bash
case $1 in
"start"){
	for i in Flink01 Flink02 Flink03
	do
        echo ---------- zookeeper $i 启动 ------------
		ssh $i "/opt/module/zookeeper/bin/zkServer.sh start"
	done
};;
"stop"){
	for i in Flink01 Flink02 Flink03
	do
        echo ---------- zookeeper $i 停止 ------------    
		ssh $i "/opt/module/zookeeper/bin/zkServer.sh stop"
	done
};;
"status"){
	for i in Flink01 Flink02 Flink03
	do
        echo ---------- zookeeper $i 状态 ------------    
		ssh $i "/opt/module/zookeeper/bin/zkServer.sh status"
	done
};;
"restart"){
        for i in Flink01 Flink02 Flink03
        do
        echo ---------- zookeeper $i 重启 ------------    
                ssh $i "/opt/module/zookeeper/bin/zkServer.sh restart"
        done
};;
esac
