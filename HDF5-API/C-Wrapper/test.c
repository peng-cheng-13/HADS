#include "stdio.h"
#include "memfsWrapper.h"

int main() {
  printf("Init\n");
  nrfs fs = nrfsConnect_Wrapper("default", 0, 0);
  printf("NRFS connected\n");
  nrfsDisconnect_Wrapper(fs);
  printf("NRFS disconnected\n");
  return 1;
}
