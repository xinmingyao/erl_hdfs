-module(hdfs_drvier_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").



init_per_suite(Config)->
    

    Config    
.

end_per_suite(Config)->

    Config.

init_per_testcase(_,Config)->
    true=hdfs_driver:load_driver(),
    {ok,Port}=hdfs_driver:new(),
    [{port,Port}|Config]
    .

end_per_testcase(Config)->
    Port=?config(port,Config),
    erlang:port_close(Port),
    ok.

all()->
    [read_file,stat].

read_file(Config)->
    Port=?config(port,Config),
    Path=code:lib_dir(erl_hdfs,'test/1.txt'),
    error_logger:info_msg("~p ~p~n",[Port,Path]),
    hdfs_driver:open_file(Port,Path,[read]),
    {ok,<<"He">>}=hdfs_driver:read_file(Port,2).


mode(Config)->
    hdfs_driver:open_mode([read]).


stat(Config)->
    Port=?config(port,Config),
    Path=code:lib_dir(erl_hdfs,'test/1.txt'),
    hdfs_driver:open_file(Port,Path,[read]),
    {ok,_}=hdfs_driver:hdfs_stat(Port)
    .


