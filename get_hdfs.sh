
wget http://labs.renren.com/apache-mirror/hadoop/common/hadoop-0.20.2/hadoop-0.20.2.tar.gz
tar zxvf hadoop-0.20.2.tar.gz
wget mirror.bit.edu.cn/apache/ant/binaries/apache-ant-1.8.4-bin.tar.gz

tar zxvf apache-ant-1.8.4-bin.tar.gz

export  JAVA_HOME=/usr/lib/jvm/java-6-openjdk
export HADOOP_HOME=/home/erlang/erl_hdfs/hadoop-0.20.2
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$HADOOP_HOME/hadoop-0.20.2-core.jar:$HADOOP_HOME/lib/commons-logging-1.0.4.jar
cd hadoop-0.20.2
../apache-ant-1.8.4/bin/ant compile-c++-libhdfs -Dislibhdfs=true

