#include "alluxio_underfs_memfs_MemFileSystem.h"
#include "nrfs.h"

/*Convert JByteaArray to char**/
char* ConvertJByteaArrayToChars(JNIEnv *env, jbyteArray bytearray) {
  char *chars = NULL;
  jbyte *bytes;
  printf("Debug-libjninrfs:: ConvertJByteaArrayToChars, GetByteArrayElements ready\n");
  bytes = env->GetByteArrayElements(bytearray, JNI_FALSE);
  printf("Debug-libjninrfs:: ConvertJByteaArrayToChars, GetByteArrayElements done\n");
  int chars_len = env->GetArrayLength(bytearray);
  printf("Debug-libjninrfs:: ConvertJByteaArrayToChars, length of byteArray is %d\n", chars_len);
  chars = (char *)malloc(chars_len + 1);
  memcpy(chars, bytes, chars_len);
  chars[chars_len] = 0;
  printf("Debug-libjninrfs:: ConvertJByteaArrayToChars, memory copied\n");
  env->ReleaseByteArrayElements(bytearray, bytes, JNI_COMMIT);
  printf("Debug-libjninrfs:: ConvertJByteaArrayToChars, Release\n");
  return chars;
}

JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsConnect
  (JNIEnv *env, jobject obj)
  {
    return (jint)nrfsConnect("defalut", 0, 0);
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsDisconnect
  (JNIEnv *env, jobject)
  {
    return (jint)nrfsDisconnect(0);
  }


JNIEXPORT jlong JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsOpenFile
  (JNIEnv *env, jobject obj, jbyteArray buf, jint flags)
  {
    printf("Debug-libjninrfs:: nrfsOpenFile\n");
    char *path = ConvertJByteaArrayToChars(env, buf);
    nrfsFile file;
    if ((int)flags == 1)
        file = nrfsOpenFile(0, path, O_CREAT);
    else
        file = nrfsOpenFile(0, path, O_RDWR);
    if (file != NULL) {
        return 1L;
    }
    return 0L;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsCloseFile
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    printf("Debug-libjninrfs:: nrfsCloseFile\n");
    int ret;
    char *path = ConvertJByteaArrayToChars(env, buf);
    ret = nrfsCloseFile(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsMknod
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    printf("Debug-libjninrfs:: nrfsMknod\t");
    int ret;
    char *path = ConvertJByteaArrayToChars(env, buf);
    printf("path is %s\n", path);
    ret = nrfsMknod(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsAccess
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    printf("Debug-libjninrfs:: nrfsAccess\t");
    int ret;
    char *path = ConvertJByteaArrayToChars(env, buf);
    printf("Translated path is %s\n", path);
    ret = nrfsAccess(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsGetAttibute
  (JNIEnv *env, jobject obj, jbyteArray file, jintArray _properties)
  {
    printf("Debug-libjninrfs:: nrfsGetAttibute\n");
    FileMeta attr;
    int ret;
    char *path = ConvertJByteaArrayToChars(env, file);
    jint *properties = env->GetIntArrayElements(_properties, 0);
    ret = nrfsGetAttribute(0, path, &attr);

    properties[0] = (jint)attr.size;
    properties[1] = (jint)attr.count;
    /* isDirectory = 1 */
    //properties[1] = (jint)((attr.count == MAX_FILE_EXTENT_COUNT) ? 1 : 0);
    
    env->ReleaseIntArrayElements(_properties, properties, JNI_COMMIT);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsWrite
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jlong size, jlong offset)
  {
    int ret;
    printf("Debug-libjninrfs:: nrfsWrite once\n");
    char *path = ConvertJByteaArrayToChars(env, _path);
    printf("Debug-libjninrfs:: write to %s once, size is %ld, offset is %ld\n", path, (long) size, (long)offset);
    jbyte *buffer = env->GetByteArrayElements(_buffer, JNI_FALSE);
    // unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(_buffer);
    ret = nrfsWrite(0, path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_buffer, buffer, JNI_COMMIT);
    return ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsRead
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jlong size, jlong offset)
  {
    int ret;
    printf("Debug-libjninrfs:: nrfsRead once\n");
    char *path = ConvertJByteaArrayToChars(env, _path);
    printf("Debug-libjninrfs:: read from %s once, size is %ld,  offset is %ld\n", path, (long) size, (long)offset);
    jbyte *buffer = env->GetByteArrayElements(_buffer, JNI_FALSE);
    ret = nrfsRead(0, path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_buffer, buffer, JNI_COMMIT);
    return ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsCreateDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    printf("Debug-libjninrfs:: nrfsCreateDirectory\n");
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret =nrfsCreateDirectory(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsDelete
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    printf("Debug-libjninrfs:: nrfsDelete\n");
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret = nrfsDelete(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsRename
  (JNIEnv *env, jobject obj, jbyteArray _oldPath, jbyteArray _newPath)
  {
    printf("Debug-libjninrfs:: nrfsRename\n");
    int ret;
    char *oldPath = ConvertJByteaArrayToChars(env, _oldPath);
    char *newPath = ConvertJByteaArrayToChars(env, _newPath);
    ret = nrfsRename(0, oldPath, newPath);
    return (jint)ret;
  }


JNIEXPORT jobjectArray JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsListDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    printf("Debug-libjninrfs:: nrfsListDirectory\n");
    int ret, i;
    nrfsfilelist list;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret = nrfsListDirectory(0, path, &list);
    ret = list.count;
    printf("Dir %s contain %d files/dirs\n", (char *)path, ret);

    jclass stringClass = env->FindClass("Ljava/lang/String;");
    jobjectArray fileList = env->NewObjectArray(ret, stringClass, NULL);
    jstring tmpPath;
    for(i = 0; i < ret; i++) {
      tmpPath = env->NewStringUTF(list.tuple[i].names);
      env->SetObjectArrayElement(fileList, i, tmpPath);
    }
    return fileList;
  }

/*
JNIEXPORT jint JNICALL Java_alluxio_underfs_memfs_MemFileSystem_nrfsListDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _properties)
  {
    int ret, i;
    nrfsfilelist list;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    jbyte *properties = env->GetByteArrayElements(_properties, 0);
    ret = nrfsListDirectory(0, (char *)path, &list);

    ret = list.count;
    printf("Dir %s contain %d files/dirs\n", (char *)path, ret);
    for(i = 0; i < ret; i++)
      memcpy((void*)((char*)properties + sizeof(char) * MAX_FILE_NAME_LENGTH * i), list.tuple[i].names, sizeof(char) * MAX_FILE_NAME_LENGTH);

    env->ReleaseByteArrayElements(_path, path, 0);
    env->ReleaseByteArrayElements(_properties, properties, 0);
    return ret;
  }
*/


