#include "stdio.h"
#include "memfsWrapper.h"
#include "nrfs.h"
#include <map>
#include <iostream>
#include <string.h>


int nrfsConnect_Wrapper(const char* host, int port, int size) {
  printf("nrfsConnect_Wrapper \n");
  int ret;
  ret = nrfsConnect(host, port, size);
  return ret;
}

int nrfsDisconnect_Wrapper(nrfs fs) {
  printf("nrfsDisconnect_Wrapper \n");
  nrfsDisconnect(fs);
}

nrfsFile nrfsOpenFile_Wrapper(nrfs fs, const char* path, int flags) {
  return nrfsOpenFile(fs, path, flags);
}

int nrfsCloseFile_Wrapper(nrfs fs, nrfsFile file) {
  return nrfsCloseFile(fs, file);
}

int nrfsMknod_Wrapper(nrfs fs, const char* path) {
  return nrfsMknod(fs, path);
}

int nrfsAccess_Wrapper(nrfs fs, const char* path) {
  return nrfsAccess(fs, path);
}

int nrfsWrite_Wrapper(nrfs fs, nrfsFile file, const void* buffer, long size, long offset) {
  return nrfsWrite(fs, file, buffer, size, offset);
}

int nrfsRead_Wrapper(nrfs fs, nrfsFile file, void* buffer, long size, long offset) {
  return nrfsRead(fs, file, buffer, size, offset);
}

int nrfsCreateDirectory_Wrapper(nrfs fs, const char* path) {
  return nrfsCreateDirectory(fs, path);
}

int nrfsDelete_Wrapper(nrfs fs, const char* path) {
  return nrfsDelete(fs, path);
}

int nrfsFreeBlock_Wrapper(short node_id, long startBlock, long  countBlock) {
  return nrfsFreeBlock(node_id, startBlock, countBlock);
}

int nrfsRename_Wrapper(nrfs fs, const char* oldpath, const char* newpath) {
  return nrfsRename(fs, oldpath, newpath);
}

long nrfsGetFileSize_Wrapper(nrfs fs, const char* path) {
  FileMeta attr;
  int ret = nrfsGetAttribute(fs, (nrfsFile) path, &attr);
  if (ret != 0) {
    return -1;
  } else {
    return (long)attr.size;
  }
}

char* correctPath(const char* path) {
  char temp[200] = {0};
  //strcpy(temp, "");
  if(strcmp(path, "/") != 0) {
    strcat(temp, "/");
  }
  strcat(temp, path);
  return temp;
}
