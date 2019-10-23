#include "test.hpp"

int subtest() {
    lrucache->put(2, 200);
    printf("Put operation in subtest\n");
}

int main() {
    int RValue;
    lrucache = new cache::lru_cache<int, int>(3);
    //cache::lru_cache<int, int> lrucache(3);
    lrucache->put(1,100);
    lrucache->put(2,200);
    if (lrucache->get(1) == 100) {
	printf("Get key 1, value = 100\n");
    }
    printf("Cache size is %d\n", lrucache->size());
    RValue = lrucache->put(3,300);
    if (RValue != NULL)
        printf("Replaced value is %d\n", RValue);
    printf("Cache size is %d\n", lrucache->size());
    lrucache->put(4,300);
    printf("Cache size is %d\n", lrucache->size());
    lrucache->put(1, 500);
    RValue = lrucache->put(5, 500);
    for(int i = 0; i < 6; i++) {
        if(lrucache->exists(i))
            printf("Key %d exists\n", i);
    }
    if (RValue != NULL)
	printf("Replaced value is %d\n", RValue);
    if (lrucache->exists(8))
        lrucache->get(8);
    subtest();
    return 1;
}
