%% @author Kevin Smith <ksmith@basho.com>
%% @copyright 2009-2010 Basho Technologies
%%
%%    Licensed under the Apache License, Version 2.0 (the "License");
%%    you may not use this file except in compliance with the License.
%%    You may obtain a copy of the License at
%%
%%        http://www.apache.org/licenses/LICENSE-2.0
%%
%%    Unless required by applicable law or agreed to in writing, software
%%    distributed under the License is distributed on an "AS IS" BASIS,
%%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%    See the License for the specific language governing permissions and
%%    limitations under the License.

%% @doc This module manages all of the low-level details surrounding the
%% linked-in driver. It is reponsible for loading and unloading the driver
%% as needed. This module is also reponsible for creating and destroying
%% instances of Javascript VMs.
 
-module(hdfs_driver).

-define(DEFAULT_HEAP_SIZE, 8). %% MB
-define(DEFAULT_THREAD_STACK, 16). %% MB
-define(DEFAULT_TIME_OUT,5000).


-export([open_mode/1]).


-export([load_driver/0, new/0, new/1, destroy/1, shutdown/1]).
-export([open_file/3,read_file/2,read_file/3]).
-export([hdfs_seek/2,hdfs_stat/1]).


-define(SCRIPT_TIMEOUT, 5000).
-define(DRIVER_NAME, "hdfs_drv").


% source from prim_file.erl
-define(FILE_OPEN,             1).
-define(FILE_READ,             2).
-define(FILE_LSEEK,            3).
-define(FILE_WRITE,            4).
-define(FILE_FSTAT,            5).
-define(FILE_PWD,              6).
-define(FILE_READDIR,          7).
-define(FILE_CHDIR,            8).
-define(FILE_FSYNC,            9).
-define(FILE_MKDIR,            10).
-define(FILE_DELETE,           11).
-define(FILE_RENAME,           12).
-define(FILE_RMDIR,            13).
-define(FILE_TRUNCATE,         14).
-define(FILE_READ_FILE,        15).
-define(FILE_WRITE_INFO,       16).
-define(FILE_LSTAT,            19).
-define(FILE_READLINK,         20).
-define(FILE_LINK,             21).
-define(FILE_SYMLINK,          22).
-define(FILE_CLOSE,            23).
-define(FILE_PWRITEV,          24).
-define(FILE_PREADV,           25).
-define(FILE_SETOPT,           26).
-define(FILE_IPREAD,           27).
-define(FILE_ALTNAME,          28).
-define(FILE_READ_LINE,        29).
-define(FILE_FDATASYNC,        30).
-define(FILE_ADVISE,           31).
-define(FILE_SENDFILE,         32).

%% Driver responses
-define(FILE_RESP_OK,          0).
-define(FILE_RESP_ERROR,       1).
-define(FILE_RESP_DATA,        2).
-define(FILE_RESP_NUMBER,      3).
-define(FILE_RESP_INFO,        4).
-define(FILE_RESP_NUMERR,      5).
-define(FILE_RESP_LDATA,       6).
-define(FILE_RESP_N2DATA,      7).
-define(FILE_RESP_EOF,         8).
-define(FILE_RESP_FNAME,       9).
-define(FILE_RESP_ALL_DATA,   10).
-define(FILE_RESP_LFNAME,     11).

%% Open modes for the driver's open function.
-define(EFILE_MODE_READ,       1).
-define(EFILE_MODE_WRITE,      2).
-define(EFILE_MODE_READ_WRITE, 3).  
-define(EFILE_MODE_APPEND,     4).
-define(EFILE_COMPRESSED,      8).
-define(EFILE_MODE_EXCL,       16).

%% Use this mask to get just the mode bits to be passed to the driver.
-define(EFILE_MODE_MASK, 31).

%% Seek modes for the driver's seek function.
-define(EFILE_SEEK_SET, 0).
-define(EFILE_SEEK_CUR, 1).
-define(EFILE_SEEK_END, 2).


-define(HDFS_CONNECT,"co").
-define(HDFS_OPEN_FILE,"of").
-define(HDFS_READ_FILE,"rd").
-define(HDFS_STAT_FILE,"st").
-define(HDFS_SEEK_FILE,"sk").
-define(HDFS_CLOSE_FILE,"cf").






%% @spec load_driver() -> true | false
%% @doc Attempt to load the Javascript driver
load_driver() ->
    {ok, Drivers} = erl_ddll:loaded_drivers(),
    case lists:member(?DRIVER_NAME, Drivers) of
        false ->
	    error_logger:info_msg("~p~n",[priv_dir()]),
            case erl_ddll:load(priv_dir(), ?DRIVER_NAME) of
                ok ->
                    true;
                {error, Error} ->
                    error_logger:error_msg("Error loading ~p: ~p~n", [?DRIVER_NAME, erl_ddll:format_error(Error)]),
                    false
            end;
        true ->
            true
    end.

%% @spec new() -> {ok, port()} | {error, atom()} | {error, any()}
%% @doc Create a new Javascript VM instance and preload Douglas Crockford's
%% json2 converter (http://www.json.org/js.html). Uses a default heap
%% size of 8MB and a default thread stack size of 8KB.
new() ->
    new(["default"]).


new(Options) ->
    Port = open_port({spawn, ?DRIVER_NAME}, [binary]),
     error_logger:info_msg("return ~p",[call_driver(Port,?HDFS_CONNECT, Options, 5000)]),
    {ok, Port}.

%% @spec destroy(port()) -> ok
%% @doc Destroys a Javascript VM instance
destroy(Ctx) ->
    port_close(Ctx).

%% @spec shutdown(port()) -> ok
%% @doc Destroys a Javascript VM instance and shuts down the underlying Javascript infrastructure.
%% NOTE: No new VMs can be created after this call is made!
shutdown(Ctx) ->
    call_driver(Ctx, ?HDFS_CLOSE_FILE, [],?DEFAULT_TIME_OUT),
    port_close(Ctx).

open_file(Ctx, FileName,Modes)->
   % case open_mode(Modes) of
%	{Mode, _Portopts, _Setopts} ->
	    case call_driver(Ctx, ?HDFS_OPEN_FILE, [FileName,1],?DEFAULT_TIME_OUT) of
		{error, ErrorJson} when is_binary(ErrorJson) ->
		    {error, ErrorJson};
		ok ->
		    ok
	    end.		
%	Reason ->
%	    {error, Reason}
%    end.

read_file(Ctx,Len)->
    read_file(Ctx,Len,?DEFAULT_TIME_OUT).
read_file(Ctx,Len, Timeout)->
    case call_driver(Ctx, ?HDFS_READ_FILE, [Len],Timeout) of
        {error, Error}  ->
            {error, Error};
        Ret ->
            Ret
    end.

hdfs_seek(Ctx,OffSet)->
    case call_driver(Ctx, ?HDFS_SEEK_FILE, [OffSet], ?DEFAULT_TIME_OUT) of
        {error, Error}  ->
            {error, Error};
        Ret ->
            Ret
    end.

hdfs_stat(Ctx)->
    case call_driver(Ctx, ?HDFS_STAT_FILE, [], ?DEFAULT_TIME_OUT) of
        {error, Error}  ->
            {error, Error};
        Ret ->
            Ret
    end.

%% @private
priv_dir() ->
    %% Hacky workaround to handle running from a standard app directory
    %% and .ez package
    case code:priv_dir(erl_hdfs) of
        {error, bad_name} ->
            filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]);
        Dir ->
            Dir
    end.

%% @private
call_driver(Ctx, Command, Args, Timeout) ->
    CallToken = make_call_token(),
    Marshalled = js_drv_comm:pack(Command, [CallToken] ++ Args),
    port_command(Ctx, Marshalled),
    Result = receive
                 {CallToken, R1} when is_atom(R1)->
                     R1;
                 {CallToken, ok, R} ->
                     {ok, R};
                 {CallToken, error, Error} ->
                     {error, Error}
             after Timeout ->
                     {error, timeout}
             end,
    Result.



%% @private
make_call_token() ->
    list_to_binary(integer_to_list(erlang:phash2(erlang:make_ref()))).





%% Converts a list of mode atoms into a mode word for the driver.
%% Returns {Mode, Portopts, Setopts} where Portopts is a list of 
%% options for erlang:open_port/2 and Setopts is a list of 
%% setopt commands to send to the port, or error Reason upon failure.

open_mode(List) when is_list(List) ->
    case open_mode(List, 0, [], []) of
	{Mode, Portopts, Setopts} when Mode band 
			  (?EFILE_MODE_READ bor ?EFILE_MODE_WRITE) 
			  =:= 0 ->
	    {Mode bor ?EFILE_MODE_READ, Portopts, Setopts};
	Other ->
	    Other
    end.
open_mode([raw|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode, Portopts, Setopts);
open_mode([read|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode bor ?EFILE_MODE_READ, Portopts, Setopts);
open_mode([write|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode bor ?EFILE_MODE_WRITE, Portopts, Setopts);
open_mode([binary|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode, [binary | Portopts], Setopts);
open_mode([compressed|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode bor ?EFILE_COMPRESSED, Portopts, Setopts);
open_mode([append|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode bor ?EFILE_MODE_APPEND bor ?EFILE_MODE_WRITE, 
	      Portopts, Setopts);
open_mode([exclusive|Rest], Mode, Portopts, Setopts) ->
    open_mode(Rest, Mode bor ?EFILE_MODE_EXCL, Portopts, Setopts);
open_mode([delayed_write|Rest], Mode, Portopts, Setopts) ->
    open_mode([{delayed_write, 64*1024, 2000}|Rest], Mode,
	      Portopts, Setopts);

open_mode([], Mode, Portopts, Setopts) ->
    {Mode, reverse(Portopts), reverse(Setopts)};
open_mode(_, _Mode, _Portopts, _Setopts) ->
    badarg.


reverse(X) -> lists:reverse(X, []).
reverse(L, T) -> lists:reverse(L, T).
