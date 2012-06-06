
#ifndef __HDFS_DRV_
#define __HDFS_DRV_
#include "hdfs.h"
#include "erl_driver.h"
#include "ei.h"
#include "string.h"
#include "assert.h"
#include "driver_comm.h"
#define CMD_OPEN_FILE 1
#define CMD_READ_FILE 2
#define CMD_CLOSE_FILE 3

typedef struct _hdfs_drv_t {
  ErlDrvPort port;
  hdfsFS fs;
  hdfsFile hdfs_file;
  char * file_name;
  ErlDrvTermData atom_ok;
  ErlDrvTermData atom_error;
  ErlDrvTermData atom_unknown_cmd;
  int close;
} hdfs_drv_t;


#define  call_return_string 0
#define  call_return_bin 1


typedef struct _hdfs_call_t {
  hdfs_drv_t *driver_data;
  ErlDrvBinary *args;
  ErlDrvTermData return_terms[20];
  char return_call_id[32];
  int return_term_count;
  const char *return_string;
  int return_type;
} hdfs_call;

typedef void (*asyncfun)(void *);

static ErlDrvData hdfs_drv_start(ErlDrvPort port,char* cmd);
static void hdfs_drv_stop(ErlDrvData handle);
static void hdfs_drv_process(ErlDrvData handle, ErlIOVec *ev);

//static void hdfs_open_file(hdfs_call *call_data);
//static void hdfs_read_file(hdfs_call *call_data);

#endif
