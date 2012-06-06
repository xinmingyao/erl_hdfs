
-record(hdfs_file_info,
	{ %% code from kenerl file.h
	  name:: string(), %name of the file
	  replication::non_neg_integer(),%% replications of the file
	  block_size::non_neg_integer(), % block size of the file
	  size   :: non_neg_integer(),	% Size of file in bytes.
	  type   :: 'device' | 'directory' | 'other' | 'regular' | 'symlink',
	  access :: 'read' | 'write' | 'read_write' | 'none',
	  atime  :: file:date_time() | integer(), % The local time the file was last read:
					         % {{Year, Mon, Day}, {Hour, Min, Sec}}.
						 % atime, ctime, mtime may also be unix epochs()
	  mtime  :: file:date_time() | integer(), % The local time the file was last written.
	  ctime  :: file:date_time() | integer(), % The interpretation of this time field
					% is dependent on operating system.
					% On Unix it is the last time the file
					% or the inode was changed.  On Windows,
					% it is the creation time.
	  mode   :: integer(),		% File permissions.  On Windows,
	 				% the owner permissions will be
					% duplicated for group and user.
	  links  :: non_neg_integer(),	% Number of links to the file (1 if the
					% filesystem doesn't support links).
	  major_device :: integer(),	% Identifies the file system (Unix),
	 				% or the drive number (A: = 0, B: = 1)
					% (Windows).
	  %% The following are Unix specific.
	  %% They are set to zero on other operating systems.
	  minor_device :: integer(),	% Only valid for devices.
	  inode  :: integer(),  		% Inode number for file.
	  uid    :: integer(),  		% User id for owner.
	  gid    :: integer()}).	        % Group id for owner.


%%%% hdfs file info struct of hdfs.h
%    typedef struct  {
%        tObjectKind mKind;   /* file or directory */
%        char *mName;         /* the name of the file */
%        tTime mLastMod;      /* the last modification time for the file in seconds */
%        tOffset mSize;       /* the size of the file in bytes */
%        short mReplication;    /* the count of replicas */
%        tOffset mBlockSize;  /* the block size for the file */
%        char *mOwner;        /* the owner of the file */
%        char *mGroup;        /* the group associated with the file */
%        short mPermissions;  /* the permissions associated with the file */
%        tTime mLastAccess;    /* the last access time for the file in seconds */
%    } hdfsFileInfo;
%%%%



