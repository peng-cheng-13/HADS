#include "RPCServer.hpp"
#include <sys/wait.h>  
#include <sys/types.h>

RPCServer *server;

/* Catch ctrl-c and destruct. */
void Stop (int signo) {
    delete server;
    Debug::notifyInfo("DMFS is terminated, Bye.");
    _exit(0);
}
int main(int argc, char** argv) {
    signal(SIGINT, Stop);
    server = new RPCServer(2, argc, argv);
    char *p = (char *)server->getMemoryManagerInstance()->getDataAddress();
    while (true) {
    	getchar();
    	printf("storage addr = %lx\n", (long)p);
    	for (int i = 0; i < 12; i++) {
    		printf("%c", p[i]);
    	}
    	printf("\n");
    }
}
