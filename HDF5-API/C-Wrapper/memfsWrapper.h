#ifndef _MEMFSWRAPPER_H_
#define _MEMFSWRAPPER_H_

#include <time.h>

#define MAX_FILE_EXTENT_COUNT 512
#define BLOCK_SIZE (16 * 1024 * 1024)
#define MAX_FILE_NAME_LENGTH 50
#define MAX_DIRECTORY_COUNT 60

typedef int nrfs;
typedef char* nrfsFile;


#ifdef __cplusplus
extern "C" {
#endif

nrfs nrfsConnect_Wrapper(const char* host, int port, int size);

int nrfsDisconnect_Wrapper(nrfs fs);

nrfsFile nrfsOpenFile_Wrapper(nrfs fs, const char* path, int flags);

int nrfsCloseFile_Wrapper(nrfs fs, nrfsFile file);

int nrfsMknod_Wrapper(nrfs fs, const char* path);

int nrfsAccess_Wrapper(nrfs fs, const char* path);

int nrfsWrite_Wrapper(nrfs fs, nrfsFile file, const void* buffer, long size, long offset);

int nrfsRead_Wrapper(nrfs fs, nrfsFile file, void* buffer, long size, long offset);

int nrfsCreateDirectory_Wrapper(nrfs fs, const char* path);

int nrfsDelete_Wrapper(nrfs fs, const char* path);

int nrfsFreeBlock_Wrapper(short node_id, long startBlock, long  countBlock);

int nrfsRename_Wrapper(nrfs fs, const char* oldpath, const char* newpath);

long nrfsGetFileSize_Wrapper(nrfs fs, const char* path);

char* correctPath(const char* path);

#ifdef __cplusplus
}
#endif

#endif
