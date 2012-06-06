%%%-------------------------------------------------------------------
%%% @author  <>
%%% @copyright (C) 2012, 
%%% @doc

%%%
%%% @end
%%% Created : 25 May 2012 by  <>
%%%-------------------------------------------------------------------
-module(ehdfs).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {port}).

-define(CMD_OPEN_FILE ,1).
-define(CMD_READ_FILE ,2).
-define(CMD_CLOSE_FILE,3).
-export([open_file/2,read_file/2]).


%%%===================================================================
%%% API
%%%===================================================================

open_file(Pid,Path)->
    gen_server:call(Pid,{open_file,Path},10000).
read_file(Pid,Len)->
    gen_server:call(Pid,{read_file,Len}).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link( ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------

init([]) ->
    PrivDir=get_priv_dir(),
    case erl_ddll:load(PrivDir,"hdfs_drv") of
	ok->
            Port = open_port({spawn, open_port_cmd("default")}, [binary]),
            {ok, #state{port = Port}};
        {error, permanent} -> %% already loaded!
            Port = open_port({spawn, open_port_cmd("default")}, [binary]),
            {ok, #state{port = Port}};            
        {error, Error} ->
            Msg = io_lib:format("Error loading ~p: ~s", 
                                ["hdfs_drv", erl_ddll:format_error(Error)]),
            {stop, lists:flatten(Msg)}
    end.

open_port_cmd(Options) ->
    "hdfs_drv"
    .

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({open_file,Path}, _From, State=#state{port=Port})->
    error_logger:info_msg("port:~p  ~p~n",[erlang:is_port(Port),erlang:port_info(Port)]),
    Port!{self(),{command,term_to_binary([?CMD_OPEN_FILE,Path])}},   
%    erlang:port_control(Port,?CMD_OPEN_FILE,"/ddd"),  
    %erlang:port_command(Port,[?CMD_OPEN_FILE,Path]),
    Reply = wait_result(Port),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_priv_dir() ->
    case code:priv_dir(sqlite3) of
        {error, bad_name} ->
            %% application isn't in path, fall back
            {?MODULE, _, FileName} = code:get_object_code(?MODULE),
            filename:join(filename:dirname(FileName), "../priv");
        Dir ->
            Dir
    end.

wait_result(Port) ->
    receive
        {Port, Reply} ->
            case Reply of
                {error, Code, Reason} ->
                    error_logger:error_msg("sqlite3 driver error: ~s~n", 
                                           [Reason]),
                    % ?dbg("Error: ~p~n", [Reason]),
                    {error, Code, Reason};
                _ ->
                    % ?dbg("Reply: ~p~n", [Reply]),
                    Reply
            end;
        {'EXIT', Port, Reason} ->
            error_logger:error_msg("sqlite3 driver port closed with reason ~p~n", 
                                   [Reason]),
            % ?dbg("Error: ~p~n", [Reason]),
            {error, Reason};
        Other when is_tuple(Other), element(1, Other) =/= '$gen_call', element(1, Other) =/= '$gen_cast' ->
            error_logger:error_msg("sqlite3 unexpected reply ~p~n", 
                                   [Other]),
            Other;
	O ->O
    after 5000
	      -> no_msg
		     
    end.
