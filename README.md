erl_hdfs
========

erlang hdfs client 


install:
	install jdk

	./get_hdfs.sh

modify rebar.config for include and so
 
add share path in /etc/ld.so.conf (ubuntu)

                     /usr/lib/jvm/java-6-openjdk/jre/lib/i386/

                     /usr/lib/jvm/java-6-openjdk/jre/lib/i386/server/

./rebar compile

./rebar ct
