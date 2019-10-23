#include "kcdirdb.h"
#include <kcpolydb.h>
#include "stdlib.h"
#include "stdio.h"

int main() {
  printf("KC Test\n");
  kyotocabinet::DirDB db;
  std::string path = "tmp/KCDB";
  if (!db.open("/tmp/KCDB", kyotocabinet::DirDB::OWRITER | kyotocabinet::DirDB::OCREATE | kyotocabinet::DirDB::OTRUNCATE)) {
    printf("DB open failed\n");
    std::cerr << "open error: " << db.error().name() << std::endl;
    return -1;
  } else {
    printf("Open KC DB \n");
  }
  const char *key = "key1";
  const char *value = "123";
  db.set((const char *)key, sizeof(key), (const char *)value, sizeof(value));
  printf("Insert key to KC DB\n");
  char v2[100];
  db.get((const char *)key, sizeof(key), (char *)v2, sizeof(v2));
  printf("Get value from KC DB once, key is %s, value is %s\n", key, v2);
  db.close();
  return 1;
}
