#!/bin/bash
source /etc/profile &> /dev/null
script_dir=$(cd "$(dirname "$0")"; pwd)
base_dir=$(pwd)/..
echo "base_dir: $base_dir"

echo "Log dir: $base_dir/logs/server.log"

if [ ! -d ../logs ];then
    mkdir ../logs
fi
nohup java -cp "$base_dir/config:$base_dir/lib/*" teleporter.web.Main >>${base_dir}/logs/server.log 2>&1 &