@echo on
setlocal enabledelayedexpansion

title lightingminer
set "CURRENT_DIR=%cd%"
cd ..
set "MINER_HOME=%cd%"
cd "%CURRENT_DIR%"

set "JVM_OPTS= -Dfile.encoding=utf-8 -Duser.timezone=GMT+8 -server -Xms200m -Xmx500m -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=5 -XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+HeapDumpOnOutOfMemoryError"


echo Starting .........


for %%i in (%MINER_HOME%\lib\*) do set CLASS_PATH=%%i;!CLASS_PATH!
echo %CLASS_PATH%
set CLASS_PATH=.;%MINER_HOME%;%CLASS_PATH%
echo CLASS_PATH=%CLASS_PATH%


java  %JVM_OPTS% -classpath %CLASS_PATH%  com.creditease.ns.miner.flume.startup.AgentLaunger > %MINER_HOME%/log/stdout.log 2>&1
