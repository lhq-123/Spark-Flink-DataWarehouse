#!/bin/bash
echo "========== 生成业务数据=========="
cd /opt/module/data/; java -jar db_log.jar >/dev/null 2>&1 &
