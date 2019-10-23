#include "Hello.h"
#include "stdio.h"
#include "stdlib.h"
/*
 *  * Class:     Hello
 *   * Method:    sayHello
 *    * Signature: ()V
 *     */
JNIEXPORT void JNICALL Java_Hello_sayHello
  (JNIEnv * env, jclass cls) {
    printf("hello from c++\n");
}
