-module(hdfs_file).
-export([open/2,pread/3,position/2,close/2,read_file_info/1]).
-include_lib("kernel/include/file.hrl").
-include("hdfs_main.hrl").

%% data types
-type filename()  :: string() | binary().
-type file_info() :: #file_info{}.
-type fd()        :: #file_descriptor{}.
-type io_device() :: pid() | fd().
-type location()  :: integer() | {'bof', Offset :: integer()}
                   | {'cur', Offset :: integer()}
		   | {'eof', Offset :: integer()} | 'bof' | 'cur' | 'eof'.
-type mode()      :: 'read' | 'write' | 'append'
                   | 'exclusive' | 'raw' | 'binary'
		   | {'delayed_write',
                      Size :: non_neg_integer(),
                      Delay :: non_neg_integer()}
		   | 'delayed_write' | {'read_ahead', Size :: pos_integer()}
		   | 'read_ahead' | 'compressed'
		   | {'encoding', unicode:encoding()}.
-type deep_list() :: [char() | atom() | deep_list()].
-type name()      :: string() | atom() | deep_list() | (RawFilename :: binary()).
-type posix()     :: 'eacces'  | 'eagain'  | 'ebadf'   | 'ebusy'  | 'edquot'
		   | 'eexist'  | 'efault'  | 'efbig'   | 'eintr'  | 'einval'
		   | 'eio'     | 'eisdir'  | 'eloop'   | 'emfile' | 'emlink'
		   | 'enametoolong'
		   | 'enfile'  | 'enodev'  | 'enoent'  | 'enomem' | 'enospc'
		   | 'enotblk' | 'enotdir' | 'enotsup' | 'enxio'  | 'eperm'
		   | 'epipe'   | 'erofs'   | 'espipe'  | 'esrch'  | 'estale'
		   | 'exdev'.
-type date_time() :: calendar:datetime().
-type posix_file_advise() :: 'normal' | 'sequential' | 'random'
                           | 'no_reuse' | 'will_need' | 'dont_need'.
-type sendfile_option() :: {chunk_size, non_neg_integer()}.
-type file_info_option() :: {'time', 'local'} | {'time', 'universal'} 
			  | {'time', 'posix'}.


-spec open(Path::string(),Modes::mode())->
		  {ok,Port::port()}|{error,posix()}.
open(Path,Modes)->
    {ok,Port}=hdfs_driver:new(),
    case hdfs_driver:open_file(Port,Path,Modes) of
	ok->{ok,Port};
	Err->Err
    end
    .

-spec pread(IoDevice,At,Size) -> {ok, DataL} | eof | {error, Reason} when
      IoDevice :: io_device(),
      At::location(),
      Size::integer(),
      DataL :: [Data],
      Data :: string() | binary() | eof,
      Reason :: posix() | badarg | terminated.

pread(Port,At,Size)->
    error_logger:info_msg("pread:~p ~p ~p",[Port,At,Size]),
   R= case position(Port,At) of
	ok->hdfs_driver:read_file(Port,Size);
	Result->Result
      end,
    error_logger:info_msg("%pread~p~n:",[R]),
    R.

-spec position(IoDevice, Location) -> {ok, NewPosition} | {error, Reason} when
      IoDevice :: io_device(),
      Location :: location(),
      NewPosition :: integer(),
      Reason :: posix() | badarg | terminated.

position(Port, At)->
    hdfs_driver:hdfs_seek(Port,At).

-spec close(Port::port(),Path::string())->
		   ok.
close(Port,Path)->
    todo.

%for compatitable with erlang file module
-spec read_file_info(Path::string())->
			    {ok,#file_info{}}|{error,Reason::any()}.
read_file_info(Path)->
    {ok,Port}=open(Path,[read]),
    {ok,{_Name,_Replication,_BlockSize,Size,_Filetype}}=hdfs_driver:hdfs_stat(Port),
    %todo close Port
    F=#file_info{size=Size},
    {ok,F}.
%% todo read hdfs file info
%% hdfs_read_file_info(Path)


