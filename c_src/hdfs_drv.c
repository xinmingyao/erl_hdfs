/* author Kevin Smith <ksmith@basho.com>
   copyright 2009-2010 Basho Technologies

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

/*
modify by yaoxinming@gmail.com 
for hdfs api
*/
#include "hdfs_drv.h"
#include "search.h"
#define EFILE_MODE_READ       1
#define EFILE_MODE_WRITE      2
#define EFILE_MODE_READ_WRITE  3  

void send_immediate_ok_response(hdfs_drv_t *dd, const char *call_id) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY, (ErlDrvTermData) call_id, strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_ok,
                            ERL_DRV_TUPLE, 2};
  driver_output_term(dd->port, terms, sizeof(terms) / sizeof(terms[0]));
}

#define COPY_DATA(CD, CID, TERMS)                                         \
    do {                                                                  \
         assert(strlen(CID) < sizeof(CD->return_call_id) - 1); \
         strcpy(CD->return_call_id, CID);                      \
         assert(sizeof(TERMS) <= sizeof(CD->return_terms));        \
         memcpy(CD->return_terms, TERMS, sizeof(TERMS));           \
         CD->return_term_count = sizeof(TERMS) / sizeof(TERMS[0]); \
    } while (0)

void send_ok_response(hdfs_drv_t *dd, hdfs_call *call_data,
                      const char *call_id) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_ok,
                            ERL_DRV_TUPLE, 2};
  COPY_DATA(call_data, call_id, terms);
}

void send_eof_response(hdfs_drv_t *dd, hdfs_call *call_data,
                      const char *call_id) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, driver_mk_atom((char *) "eof"),
                            ERL_DRV_TUPLE, 2};
  COPY_DATA(call_data, call_id, terms);
}


void send_error_string_response(hdfs_drv_t *dd, hdfs_call *call_data,
                                const char *call_id, const char *msg) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_error,
                            ERL_DRV_BUF2BINARY, (ErlDrvTermData) msg, strlen(msg),
                            ERL_DRV_TUPLE, 3};
  COPY_DATA(call_data, call_id, terms);
  call_data->return_string = msg;
}

void send_string_response(hdfs_drv_t *dd, hdfs_call *call_data,
                          const char *call_id, const char *result) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_ok,
                            ERL_DRV_BUF2BINARY, (ErlDrvTermData) result, strlen(result),
                            ERL_DRV_TUPLE, 3};
  COPY_DATA(call_data, call_id, terms);
  call_data->return_string = result;
}

// {id,ok,name,replication,block_size,size,type}
void send_fileinfo_response(hdfs_drv_t *dd, hdfs_call *call_data,
			    const char *call_id, const hdfsFileInfo *file_info) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_ok,
                            ERL_DRV_BUF2BINARY,(ErlDrvTermData)file_info->mName,strlen(file_info->mName),
			    ERL_DRV_INT,file_info->mReplication,
			    ERL_DRV_INT,file_info->mBlockSize,
			    ERL_DRV_INT,file_info->mSize,
			    ERL_DRV_ATOM,file_info->mKind==kObjectKindFile?driver_mk_atom("f"):driver_mk_atom("d"),
			    ERL_DRV_TUPLE,5,
			    ERL_DRV_TUPLE, 3};
  COPY_DATA(call_data, call_id, terms);
  
}  

void send_binary_response(hdfs_drv_t *dd, hdfs_call *call_data,
                          const char *call_id, const ErlDrvBinary *result,int len) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_ok,
                            ERL_DRV_BINARY,result, len,0,
                            ERL_DRV_TUPLE, 3};
  COPY_DATA(call_data, call_id, terms);
  call_data->return_string = result;
  call_data->return_type=call_return_bin;
}


void unknown_command(hdfs_drv_t *dd, hdfs_call *call_data,
                     const char *call_id) {
  ErlDrvTermData terms[] = {ERL_DRV_BUF2BINARY,
                            (ErlDrvTermData) call_data->return_call_id,strlen(call_id),
                            ERL_DRV_ATOM, dd->atom_error,
                            ERL_DRV_ATOM, dd->atom_unknown_cmd,
                            ERL_DRV_TUPLE, 3};
  COPY_DATA(call_data, call_id, terms);
}

char *copy_string(const char *source) {
  size_t size = strlen(source) + 1;
  char *retval = driver_alloc((ErlDrvSizeT) size);
  memset(retval, 0, size);
  strncpy(retval, source, size - 1);
  return retval;
}

void run_hdfs(void *jsargs) {
  hdfsFile file;
  tSize r_size;
  hdfs_call *call_data = (hdfs_call *) jsargs;
  hdfs_drv_t *dd = call_data->driver_data;
  ErlDrvBinary *args = call_data->args;
  char *data = args->orig_bytes;
  char *command = read_command(&data);
  char *call_id = read_string(&data);
  char *result = NULL;
  if (strncmp(command, "co", 2) == 0) {//connect to hdfs
    dd->fs=hdfsConnect("default", 0);
    if (dd->fs == NULL) {
      result=copy_string("error"),
      send_error_string_response(dd, call_data, call_id,result);
    }
    else {
      // hdfsDisconnect(dd->fs);
      // dd->hdfs_drv=fs;
      send_ok_response(dd, call_data, call_id);
    }
  }else if (strncmp(command, "of", 2) == 0) {
    char *filename = read_string(&data);
    int mode = read_int32(&data);
    if(mode == EFILE_MODE_READ){
      file = hdfsOpenFile(dd->fs,filename, O_RDONLY, 0, 0, 0);
    }else if(mode == EFILE_MODE_WRITE){
      file = hdfsOpenFile(dd->fs,filename, O_WRONLY|O_CREAT, 0, 0, 0);
    }else{
      file = hdfsOpenFile(dd->fs,filename, O_RDONLY, 0, 0, 0);//default open file todo fixed??
    }
    if (file==NULL) {
      result=copy_string("open file error");
      send_error_string_response(dd, call_data, call_id, result);
    }
    else {
      dd->hdfs_file=file;
      dd->file_name =copy_string(filename);
      send_ok_response(dd, call_data, call_id);
    }
    driver_free(filename);
  }
  else if (strncmp(command, "rd", 2) == 0) {
    int  length = read_int32(&data);
    ErlDrvBinary *bin = (ErlDrvBinary *)driver_alloc_binary(length);
    driver_binary_inc_refc(bin);
    //char * buffer= bin->orig_bytes;
    r_size = hdfsRead(dd->fs,dd->hdfs_file,(void*)bin->orig_bytes, length);
    if (r_size >0) {
      // DBG("open"),
      send_binary_response(dd, call_data, call_id, bin,r_size);
    }
    else if(r_size<0){
      result=copy_string("read file error");
      send_error_string_response(dd, call_data, call_id, result);
    }else{
      send_eof_response(dd, call_data, call_id);
    }
    
    driver_free_binary(bin);
  }
  else if (strncmp(command, "st", 2) == 0) {//stat for file_info
    hdfsFileInfo * hdfs_info=hdfsGetPathInfo(dd->fs,dd->file_name);
    if(hdfs_info ==NULL){
      send_error_string_response(dd,call_data,call_id,copy_string("/tmp/1.txt"));
    }else{
      //send fileinfo to erlang emulate
      send_fileinfo_response(dd,call_data,call_id,hdfs_info);
      hdfsFreeFileInfo(hdfs_info, 1);
      
    }
  }else if (strncmp(command, "sk", 2) == 0) {//seek the file
    
    int  position = read_int32(&data);
    int rt= hdfsSeek(dd->fs,dd->hdfs_file,position);
    if(rt ==-1){
      send_error_string_response(dd,call_data,call_id,copy_string("sk error"));
    }else{ //ok
      send_ok_response(dd,call_data,call_id);    
    }
  } else if (strncmp(command, "cf", 2) == 0) {
    dd->close = 1;
    int rt= hdfsCloseFile(dd->fs, dd->hdfs_file);
    if(rt==0){
      send_ok_response(dd, call_data, call_id);
    }else{
       send_error_string_response(dd,call_data,call_id,copy_string("close file error"));
    }
  } else {
    unknown_command(dd, call_data, call_id);
  }
  driver_free(command);
  driver_free(call_id);
  
}

static int init(void) {
  //sm_configure_locale();
  return 0;
}

static ErlDrvData hdfs_drv_start(ErlDrvPort port, char *cmd) {
  hdfs_drv_t *retval = (hdfs_drv_t*) driver_alloc((ErlDrvSizeT) sizeof(hdfs_drv_t));
  retval->port = port;
  retval->close = 0;
  //  retval->return_type = call_return_string;
  retval->atom_ok = driver_mk_atom((char *) "ok");
  retval->atom_error = driver_mk_atom((char *) "error");
  retval->atom_unknown_cmd = driver_mk_atom((char *) "unknown_command");
  return (ErlDrvData) retval;
}

static void hdfs_drv_stop(ErlDrvData handle) {
  hdfs_drv_t *dd = (hdfs_drv_t*) handle;
  if(dd !=NULL){
    
     if(dd->close==0&&dd->fs!=NULL&&dd->hdfs_file!=NULL){//not close
      hdfsCloseFile(dd->fs,dd->hdfs_file);
     }
    if(dd->fs!=NULL)
      hdfsDisconnect(dd->fs);
    driver_free(dd);
    hdestroy();
  }
}

static void hdfs_drv_process(ErlDrvData handle, ErlIOVec *ev) {
  hdfs_drv_t *dd = (hdfs_drv_t *) handle;
  //char *data = ev->binv[1]->orig_bytes;
  hdfs_call *call_data = (hdfs_call *) driver_alloc((ErlDrvSizeT) sizeof(hdfs_call));
  call_data->driver_data = dd;
  call_data->args = ev->binv[1];
  call_data->return_terms[0] = 0;
  call_data->return_term_count = 0;
  call_data->return_string = NULL;
  call_data->return_type = call_return_string;
  driver_binary_inc_refc(call_data->args);
   ErlDrvPort port = dd->port;
   intptr_t port_ptr = (intptr_t) port;
   unsigned int thread_key = port_ptr;
  driver_async(dd->port, &thread_key, (asyncfun) run_hdfs, (void *) call_data, NULL);
 
 
}

static void
ready_async(ErlDrvData handle, ErlDrvThreadData async_data)
{
  hdfs_drv_t *dd = (hdfs_drv_t *) handle;
  hdfs_call *call_data = (hdfs_call *) async_data;

  driver_output_term(dd->port,
                   call_data->return_terms, call_data->return_term_count);

  driver_free_binary(call_data->args);
  if(call_data->return_type==call_return_string){
    if (call_data->return_string != NULL) {
      driver_free((void *) call_data->return_string);
    }
  }
  if(call_data->return_type==call_return_bin){
    if (call_data->return_string != NULL) {
      driver_free_binary((void *) call_data->return_string);
    }
  }
  
  driver_free(call_data);
}



static ErlDrvEntry hdfs_drv_entry = {
    init,                             /* init */
    hdfs_drv_start,                            /* startup */
    hdfs_drv_stop,                             /* shutdown */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    (char *) "hdfs_drv",         /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    NULL,                             /* control */
    NULL,                             /* timeout */
    hdfs_drv_process,                          /* process */
    ready_async,                      /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL,                             /* event */
    ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MINOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};


DRIVER_INIT(hdfs_drv) {
  return &hdfs_drv_entry;
}

