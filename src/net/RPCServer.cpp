#include "RPCServer.hpp"
// __thread struct  timeval startt, endd;
RPCServer::RPCServer(int _cqSize, int argc, char** argv) :cqSize(_cqSize) {

	MPI_Init(&argc, &argv);
	/*
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	printf("MPI Init success!, my rank is %d\n", rank);
	if (rank == 0) {
  	  TestSend();
	} else {
	  TestRecv();
	}
	*/
	MPI_Finalize();

	requestCount = 0;
	ReplytoClient = false;
	mm = 0;
	UnlockWait = false;
	conf = new Configuration();
	mem = new MemoryManager(mm, conf->getServerCount(), 512);
	mm = mem->getDmfsBaseAddress();
	Debug::notifyInfo("DmfsBaseAddress = %lx, DmfsTotalSize = %ld",
		mem->getDmfsBaseAddress(), (long) mem->getDmfsTotalSize());
	ServerCount = conf->getServerCount();
	socket = new RdmaSocket(cqSize, mm, mem->getDmfsTotalSize(), conf, true, 0);
	client = new RPCClient(conf, socket, mem, (uint64_t)mm);
	tx = new TxManager(mem->getLocalLogAddress(), mem->getDistributedLogAddress());
	socket->RdmaListen();
        printf("Debug-RPCServer.cpp: countNode = %ld, NodeHash = %ld \n", (long)conf->getServerCount(), (long)socket->getNodeID());
	fs = new FileSystem((char *)mem->getMetadataBaseAddress(),
              (char *)mem->getDataAddress(),
              1024 * 20,/* Constructor of file system. */
              1024 * 30,
              256,
              conf->getServerCount(),    
              socket->getNodeID());
	printf("Debug-RPCServer.cpp: ready to rootInitialize ");
        printf("Current nodeid is %d\n", (int)socket->getNodeID());
	fs->rootInitialize(socket->getNodeID());
	wk = new thread[cqSize]();
	for (int i = 0; i < cqSize; i++)
		wk[i] = thread(&RPCServer::Worker, this, i);
}
RPCServer::~RPCServer() {
	Debug::notifyInfo("Stop RPCServer.");
	delete conf;
	for (int i = 0; i < cqSize; i++) {
		wk[i].detach();
	}
	delete mem;
	delete wk;
	delete socket;
	delete tx;
	Debug::notifyInfo("RPCServer is closed successfully.");
}

RdmaSocket* RPCServer::getRdmaSocketInstance() {
	return socket;
}

MemoryManager* RPCServer::getMemoryManagerInstance() {
	return mem;
}

RPCClient* RPCServer::getRPCClientInstance() {
	return client;
}

TxManager* RPCServer::getTxManagerInstance() {
	return tx;
}

void RPCServer::TestSend() {
  char message[20];
  strcpy(message, "Hello process 1");
  MPI_Send(message, strlen(message), MPI_CHAR, 1, 99, MPI_COMM_WORLD);
  printf("Message send!\n");
}

void RPCServer::TestRecv() {
  MPI_Status status;
  char message[20];
  MPI_Recv(message, 20, MPI_CHAR, 0 ,99, MPI_COMM_WORLD, &status);
  printf("Recv message: %s\n", message);
}

void RPCServer::Worker(int id) {
	//int required=MPI_THREAD_MULTIPLE, provided;
	//MPI_Init_thread(&argc, &argv, required, &provided);
	//MPI_Finalize();
	uint32_t tid = gettid();
	// gettimeofday(&startt, NULL);
	Debug::notifyInfo("Worker %d, tid = %d", id, tid);
	th2id[tid] = id;
	mem->setID(id);
	printf("Debug-RPCServer.cpp: Ready to poll request\n");
	while (true) {
		//sleep(1);
		RequestPoller(id);
	}
}

void RPCServer::RequestPoller(int id) {
	//struct ibv_wc wc[1];
	uint16_t NodeID;
	uint16_t offset;
        bool ret = false;
	int count = 0;
	uint64_t bufferRecv;
	// unsigned long diff;
	//ret = socket->PollOnce(id, 1, wc);

	ret = socket->GlexPollOnce(&NodeID, &offset);

        if (ret) {
		Debug::debugItem("RPCServer.cpp: RequestPoller, receive request from NodeID = %d, offset = %d", NodeID, offset);
		//NodeID = (uint16_t)event->cookie_1;
		if (NodeID == 0XFFF) {
			return;
		}
		//offset = (uint16_t)event->cookie_0;
		count += 1;
		if (NodeID > 0 && NodeID <= ServerCount){
			bufferRecv = mem->getServerRecvAddress(NodeID, offset);
		}else if (NodeID > ServerCount) {
                        /* Recv Message From Client. */
                        bufferRecv = mem->getClientMessageAddress(NodeID);
                }
                GeneralSendBuffer *send = (GeneralSendBuffer*)bufferRecv;
		switch (send->message) {
			case MESSAGE_TEST: {

                        }
                        default: {
				Debug::debugItem("RPCServer.cpp: RequestPoller,Ready to ProcessRequest");
				ProcessRequest(send, NodeID, offset);
				socket->GlexDuscardOnce();
			}
		}

	} else {
		return;
	}

}

void RPCServer::ProcessQueueRequest() {
	for (auto task = tasks.begin(); task != tasks.end(); ) {
		// printf("1\n");
		ProcessRequest((GeneralSendBuffer *)(*task)->send, (*task)->NodeID, (*task)->offset);
		free(*task);
		task = tasks.erase(task);
	}
	// printf("2\n");
}

void RPCServer::ProcessRequest(GeneralSendBuffer *send, uint16_t NodeID, uint16_t offset) {
	requestCount++;

	char receiveBuffer[CLIENT_MESSAGE_SIZE];
	uint64_t bufferRecv = (uint64_t)send;
	GeneralReceiveBuffer *recv = (GeneralReceiveBuffer*)receiveBuffer;
	recv->taskID = send->taskID;
	recv->message = MESSAGE_RESPONSE;
	uint64_t size = send->sizeReceiveBuffer;
	if (send->message == MESSAGE_DISCONNECT) {
          //rdma->disconnect(send->sourceNodeID);
          return;
	} else if (send->message == MESSAGE_TEST) {
    	  ;
	} else if (send->message == MESSAGE_UPDATEMETA) {
    	/* Write unlock. */
    	// UpdateMetaSendBuffer *bufferSend = (UpdateMetaSendBuffer *)send;
    	// fs->unlockWriteHashItem(bufferSend->key, NodeID, bufferSend->offset);
      	  return;
	    } else if (send->message == MESSAGE_EXTENTREADEND) {
    	/* Read unlock */
    	// ExtentReadEndSendBuffer *bufferSend = (ExtentReadEndSendBuffer *)send;
    	// fs->unlockReadHashItem(bufferSend->key, NodeID, bufferSend->offset);
    	  return;
	} else {
     	  fs->parseMessage((char*)send, receiveBuffer);
	  Debug::debugItem("Debug-RPCServer.cpp: message has been processed");
    	// fs->recursivereaddir("/", 0);
	  Debug::debugItem("Contract Receive Buffer, size = %d.", size);
	  size -= ContractReceiveBuffer(send, recv);
    	  if (send->message == MESSAGE_RAWREAD) {
    		ExtentReadSendBuffer *bufferSend = (ExtentReadSendBuffer *)send;
    		uint64_t *value = (uint64_t *)mem->getDataAddress();
    		// printf("rawread size = %d\n", (int)bufferSend->size);
    		*value = 1;
    		socket->RdmaWrite(NodeID, mem->getDataAddress(), 2 * 4096, bufferSend->size, -1, 1);
    	  } else if (send->message == MESSAGE_RAWWRITE) {
    		ExtentWriteSendBuffer *bufferSend = (ExtentWriteSendBuffer *)send;
    		// printf("rawwrite size = %d\n", (int)bufferSend->size);
    		uint64_t *value = (uint64_t *)mem->getDataAddress();
    		*value = 0;
    		socket->RdmaRead(NodeID, mem->getDataAddress(), 2 * 4096, bufferSend->size, 1, false); // FIX ME.
    		//while (*value == 0);
    	  }
	  Debug::debugItem("Copy Reply Data, size = %d.", size);
    	  memcpy((void *)send, receiveBuffer, size);
	  Debug::debugItem("Select Buffer.");
    	  if (NodeID > 0 && NodeID <= ServerCount) {
			/* Recv Message From Other Server. */
			bufferRecv = bufferRecv - mm;
	  } else if (NodeID > ServerCount) {
			/* Recv Message From Client. */
			bufferRecv = 0;
	  }
	  Debug::debugItem("Debug-RPCServer.cpp: Copy Reply Data, send = %lx, recv = %lx", send, bufferRecv);
	  Debug::debugItem("Detect  MESSAGE_EXTENTWRITE, write without remote event");
          socket->_RdmaBatchWrite(NodeID, (uint64_t)send, bufferRecv, size, 0, 1);
	  /*
	  if (ReplytoClient) {
		Debug::debugItem("Detect  MESSAGE_EXTENTWRITE, write with remote event");
		socket->_RdmaBatchWrite(NodeID, (uint64_t)send, bufferRecv, size, 1, 1);
		ReplytoClient = false;
	  } else {
		Debug::debugItem("Detect  MESSAGE_EXTENTWRITE, write without remote event");
		socket->_RdmaBatchWrite(NodeID, (uint64_t)send, bufferRecv, size, 0, 1);
	  }
	  */
	  //socket->RdmaReceive(NodeID, mm + NodeID * 4096, 0);
	  Debug::debugItem("Debug-RPCServer.cpp: process end, requestCount is %d", requestCount);
    }
}

int RPCServer::getIDbyTID() {
	uint32_t tid = gettid();
	return th2id[tid];
}
uint64_t RPCServer::ContractReceiveBuffer(GeneralSendBuffer *send, GeneralReceiveBuffer *recv) {
	uint64_t length;
	switch (send->message) {
		case MESSAGE_GETATTR: {
			GetAttributeReceiveBuffer *bufferRecv = 
			(GetAttributeReceiveBuffer *)recv;
			if (bufferRecv->attribute.count >= 0 && bufferRecv->attribute.count < MAX_FILE_EXTENT_COUNT)
				length = (MAX_FILE_EXTENT_COUNT - bufferRecv->attribute.count) * sizeof(FileMetaTuple);
			else 
				length = sizeof(FileMetaTuple) * MAX_FILE_EXTENT_COUNT;
			break;
		}
		case MESSAGE_READDIR: {
			ReadDirectoryReceiveBuffer *bufferRecv = 
			(ReadDirectoryReceiveBuffer *)recv;
			if (bufferRecv->list.count >= 0 && bufferRecv->list.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->list.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		case MESSAGE_EXTENTREAD: {
			ReplytoClient = true;
			ExtentReadReceiveBuffer *bufferRecv = 
			(ExtentReadReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_EXTENTWRITE: {
			ReplytoClient = true;
			ExtentWriteReceiveBuffer *bufferRecv = 
			(ExtentWriteReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_READDIRECTORYMETA: {
			ReadDirectoryMetaReceiveBuffer *bufferRecv = 
			(ReadDirectoryMetaReceiveBuffer *)recv;
			if (bufferRecv->meta.count >= 0 && bufferRecv->meta.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->meta.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		default: {
			length = 0;
			break;
		}
	}	
	// printf("contract length = %d", (int)length);
	return length;
}
