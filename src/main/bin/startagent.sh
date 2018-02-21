#!/bin/bash  -vx
#author hengzhou

#jvm启动参数
#JVM_OPTS=" -Dfile.encoding=utf-8  -Duser.timezone=GMT+8 -server -Xms512m -Xmx512m  -XX:MaxPermSize=128m  -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=5 -XX:+PrintGC -XX:+PrintGCTimeStamps  -XX:+PrintGCDetails  -XX:+PrintGCApplicationStoppedTime -XX:+HeapDumpOnOutOfMemoryError "
JVM_OPTS=" -Dfile.encoding=utf-8  -Duser.timezone=GMT+8 -server -Xms200m -Xmx500m -XX:MaxDirectMemorySize=1024m   -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=5 -XX:+PrintGC -XX:+PrintGCTimeStamps  -XX:+PrintGCDetails  -XX:+PrintGCApplicationStoppedTime -XX:+HeapDumpOnOutOfMemoryError "
CLASS_PATH=""
OPTIONS=" -Dfile.encoding=utf-8"
cd ..
echo Starting .........

export LANG="en_US.UTF-8" 
for i in $PWD/lib/*;
    do CLASS_PATH=$i:"$CLASS_PATH";
done
CLASS_PATH=$PWD/config/:$CLASS_PATH
export CLASS_PATH=.:$CLASS_PATH
echo CLASS_PATH=.:$CLASS_PATH


nohup java $JVM_OPTS -classpath $CLASS_PATH  com.creditease.ns.miner.flume.startup.AgentLaunger >$PWD/log/agent_stdout.log 2>&1 &

