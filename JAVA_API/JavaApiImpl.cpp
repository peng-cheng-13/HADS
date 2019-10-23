#include "MemFileSystem.h"
#include "nrfs.h"

/*Convert JByteaArray to char**/
char* ConvertJByteaArrayToChars(JNIEnv *env, jbyteArray bytearray) {
  char *chars = NULL;
  jbyte *bytes;
  bytes = env->GetByteArrayElements(bytearray, 0);
  int chars_len = env->GetArrayLength(bytearray);
  chars = new char[chars_len + 1];
  memset(chars,0,chars_len + 1);
  memcpy(chars, bytes, chars_len);
  chars[chars_len] = 0;
  env->ReleaseByteArrayElements(bytearray, bytes, 0);
  return chars;
}

JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsConnect
  (JNIEnv *env, jobject obj)
  {
    return (jint)nrfsConnect("defalut", 0, 0);
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsDisconnect
  (JNIEnv *env, jobject)
  {
    return (jint)nrfsDisconnect(0);
  }


JNIEXPORT jlong JNICALL Java_MemFileSystem_nrfsOpenFile
  (JNIEnv *env, jobject obj, jbyteArray buf, jint flags)
  {
    char *path = ConvertJByteaArrayToChars(env, buf);
    nrfsFile file;
    if ((int)flags == 1)
        file = nrfsOpenFile(0, path, O_CREAT);
    else
        file = nrfsOpenFile(0, path, O_RDWR);
    return (jlong)file;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsCloseFile
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, buf);
    ret = nrfsCloseFile(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsMknod
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, buf);
    ret = nrfsMknod(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsAccess
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret; 
    char *path = ConvertJByteaArrayToChars(env, buf);
    printf("Debug-libjninrfs:: Translated path is %s\n", path);
    ret = nrfsAccess(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsGetAttibute
  (JNIEnv *env, jobject obj, jbyteArray file, jintArray _properties)
  {
    FileMeta attr;
    int ret;

    char *path = ConvertJByteaArrayToChars(env, file);
    jint *properties = env->GetIntArrayElements(_properties, 0);
    ret = nrfsGetAttribute(0, path, &attr);

    properties[0] = (jint)attr.size;
    properties[1] = (jint)attr.count;
    /* isDirectory = 1 */
    //properties[1] = (jint)((attr.count == MAX_FILE_EXTENT_COUNT) ? 1 : 0);
    
    env->ReleaseIntArrayElements(_properties, properties, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsWrite
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jint size, jint offset)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    jbyte *buffer = env->GetByteArrayElements(_buffer, 0);
    // unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(_buffer);
    ret = nrfsWrite(0, path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_buffer, buffer, 0);
    return ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsRead
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jint size, jint offset)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    jbyte *buffer = env->GetByteArrayElements(_buffer, 0);
    ret = nrfsRead(0, path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_buffer, buffer, 0);
    return ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsCreateDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret =nrfsCreateDirectory(0, path);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsDelete
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    int ret;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret = nrfsDelete(0, path);

    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsRename
  (JNIEnv *env, jobject obj, jbyteArray _oldPath, jbyteArray _newPath)
  {
    int ret;
    char *oldPath = ConvertJByteaArrayToChars(env, _oldPath);
    char *newPath = ConvertJByteaArrayToChars(env, _newPath);
    ret = nrfsRename(0, oldPath, newPath);
    return (jint)ret;
  }


JNIEXPORT jobjectArray JNICALL Java_MemFileSystem_nrfsListDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    int ret, i;
    nrfsfilelist list;
    char *path = ConvertJByteaArrayToChars(env, _path);
    ret = nrfsListDirectory(0, path, &list);
    if (ret != 0) {
        printf("Debug-libjninrfs:: nrfsListDirectory %s failed\n", (char *)path);
	exit(0);
    }
    ret = list.count;
    printf("Debug-libjninrfs:: Dir %s contain %d files/dirs\n", (char *)path, ret);

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
JNIEXPORT jint JNICALL Java_MemFileSystem_nrfsListDirectory
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


