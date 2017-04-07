#!/bin/bash
source /etc/profile &> /dev/null
source ~/.bash_profile &> /dev/null

script_dir=$(cd "$(dirname "$0")"; pwd)
cd ${script_dir}
base_dir=$(pwd)/..
echo "base_dir: $base_dir"

if [ ! -d ../logs ];then
    mkdir ../logs
fi

mode=$1
control=$2

case ${mode} in
    "broker")
        jvmOpts="-server -Xmx512m -Xms512m -XX:+UseG1GC"
    ;;
    "instance")
        instanceArgs=$(cat ../../instance)
        jvmOpts="-server -Xmx1024m -Xms1024m -XX:+UseG1GC ${instanceArgs}"
    ;;
esac

case ${control} in
    "start")
        nohup java ${jvmOpts} -cp "$base_dir/config:$base_dir/lib/*" teleporter.integration.cluster.Boot ${mode} >>${base_dir}/logs/server.log 2>&1 &
    ;;
    "stop")
        ps aux | grep [j]ava | grep teleporter.integration.cluster.Boot | grep ${mode} | awk '{print $2}' | xargs -n 1 kill
    ;;
    *) echo "please input start|stop, mode support broker,instance,local"
    ;;
esac