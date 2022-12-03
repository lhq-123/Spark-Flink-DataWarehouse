#! /bin/bash
 
for i in Flink01 Flink02 Flink03
do
    echo --------- $i ----------
    ssh $i "$*"
done
