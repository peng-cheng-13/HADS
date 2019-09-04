/***********************************************************************
* 
* 
* Tsinghua Univ, 2016
*
***********************************************************************/
#include "RdmaSocket.hpp"
using namespace std;

RdmaSocket::RdmaSocket(int _cqNum, uint64_t _mm, uint64_t _mmSize, Configuration* _conf, bool _isServer, uint8_t _Mode) :
DeviceName(NULL), Port(1), ServerPort(5678), GidIndex(0), 
isRunning(true), isServer(_isServer), cqNum(_cqNum), cqPtr(0), 
mm(_mm), mmSize(_mmSize), conf(_conf), MaxNodeID(1), Mode(_Mode) {
	/* Use multiple cq to parallelly process new request. */
	cq = (struct ibv_cq **)malloc(cqNum * sizeof(struct ibv_cq *));
    for (int i = 0; i < cqNum; i++)
        cq[i] = NULL;
	/* Find my IP, and initialize my NodeID (At server side). */
	/* NodeID at client side will be given on connection */
    ServerCount = conf->getServerCount();
    MaxNodeID = ServerCount + 1;
	if (isServer) {
		char hname[128];
		struct hostent *hent;
		gethostname(hname, sizeof(hname));
		hent = gethostbyname(hname);
		string ip(inet_ntoa(*(struct in_addr*)(hent->h_addr_list[0])));
		MyNodeID = conf->getIDbyIP(ip);
        Debug::notifyInfo("IP = %s, NodeID = %d", ip.c_str(), MyNodeID);
	} else {
        cqPtr = 0;
    }
	CreateResources();
    if (!isServer) {
        for (int  i = 0; i < WORKER_NUMBER; i++) {
            WriteSize[i] = 0;
            ReadSize[i] = 0;
            WriteTimeCost[i] = 0;
            ReadTimeCost[i] = 0;
        }
        WriteTest = false;
        for (int i = 0; i < WORKER_NUMBER; i ++) {
            worker[i] = thread(&RdmaSocket::DataTransferWorker, this, i);
        }
    }
}

RdmaSocket::~RdmaSocket() {
    Debug::notifyInfo("Stop RdmaSocket.");
    if (isServer) {
        Debug::debugItem("1");
        Listener.detach();
    } else {
        for (int i = 0; i < WORKER_NUMBER; i++) {
            worker[i].detach();
        }
    }
    Debug::debugItem("2");
	ResourcesDestroy();
    Debug::notifyInfo("RdmaSocket is closed successfully.");
}

void RdmaSocket::NotifyPerformance() {
    for (int i = 0; i < WORKER_NUMBER; i++) {
        printf("\n");
        Debug::notifyInfo("TotalWriteSize = %ld, WriteTimeCost = %ld", WriteSize[i], WriteTimeCost[i]);
        Debug::notifyInfo("TotalReadSize = %ld, ReadTimeCost = %ld", ReadSize[i], ReadTimeCost[i]);
    }
}

bool RdmaSocket::CreateResources() {

    /* Open GLEX device*/
    glex_ret_t ret;
    int rc = 0, mrFlags, i;
    uint32_t DevicesNum;

    ret = glex_num_of_device(&DevicesNum);
    if (ret != GLEX_SUCCESS) {
        Debug::notifyError("failed to get GLEX devices num");
        rc = 1;
        goto CreateResourcesExit;
    } else {
	if (MyNodeID == 2)
          printf("Num of glex devices is %d\n", (int)DevicesNum);
    }

    ret = glex_open_device(0, &dev);
    glex_query_device(dev, &dev_attr);
    if (ret != GLEX_SUCCESS) {
        Debug::notifyError("failed to open device");
        rc = 1;
        goto CreateResourcesExit;
    } else {
	if (MyNodeID == 2)
          printf("Device opened\n");
    }

    /*Init endpoint info*/
    struct glex_ep_attr tmpep_attr;
    tmpep_attr.type = GLEX_EP_TYPE_NORMAL;
    tmpep_attr.mpq_type        = GLEX_MPQ_TYPE_NORMAL;
    tmpep_attr.eq_type         = GLEX_EQ_TYPE_NORMAL;
    tmpep_attr.key             = 0x81;
    tmpep_attr.num = GLEX_ANY_EP_NUM;
    tmpep_attr.dq_capacity     = GLEX_EP_DQ_CAPACITY_DEFAULT;
    tmpep_attr.mpq_capacity    = GLEX_EP_MPQ_CAPACITY_DEFAULT;
    tmpep_attr.eq_capacity     = GLEX_EP_EQ_CAPACITY_DEFAULT;

    ret = glex_create_ep(dev, &tmpep_attr, &ep);
    glex_query_ep(ep, &ep_attr);
    if (ret != GLEX_SUCCESS) {
        Debug::notifyError("failed to create Endpoint");
        rc = 1;
        goto CreateResourcesExit;
    } else {
	if (MyNodeID == 2)
          printf("Endpoint created\n");
    }
    //mmSize = (uint64_t)4 * 1024 * 1024 * 1024;
    if (MyNodeID == 2)
      printf("mmSize is %ld\n", (long)mmSize);

    /*
    size_t page_size;
    page_size = sysconf(_SC_PAGESIZE);
    if (posix_memalign((void**)mm, page_size, mmSize)) {
        printf("cannot allocate memory\n");
        goto CreateResourcesExit;
    } else {
        printf("Memory aligned\n");
    }
    */

    ret = glex_register_mem(ep, (void*)mm, mmSize,
                                GLEX_MEM_READ | GLEX_MEM_WRITE,
                                &local_mh);
    if (ret != GLEX_SUCCESS) {
        Debug::notifyError("Memory registration failed");
	switch (ret) {
	    case GLEX_INVALID_PARAM :
    	        printf("GLEX INVALID PARAM\n");
		break;
	    case GLEX_NO_MEM_RESOURCE :
		printf("GLEX NO MEM RESOURCE\n");
		break;
	    case GLEX_SYS_ERR :
		printf("GLEX_SYS_ERR \n");
		break;
	    default :
	        printf("None");
	}
	  
        rc = 1;
        goto CreateResourcesExit;
    } else {
        printf("Memory registration succeed\n");
    }
    
    CreateResourcesExit:
    
    if (rc) {
        /* Error encountered, cleanup */
        Debug::notifyError("Error Encountered, Cleanup ...");

        glex_destroy_ep(ep);
        ep = NULL;


        glex_close_device(dev);
        dev = NULL;

        return false;
    }
    
    return true;
}

bool RdmaSocket::CreateQueuePair(PeerSockData *peer, int offset) {

	struct ibv_qp_init_attr attr;
	memset(&attr, 0, sizeof(attr));

	if(Mode == 0) {
        attr.qp_type = IBV_QPT_RC;
    } else if (Mode == 1) {
        attr.qp_type = IBV_QPT_UC;
    }
    attr.sq_sig_all = 0;

    if (isServer && peer->NodeID > 0 && peer->NodeID <= ServerCount) {
        /* Server interconnect, use cq at 0. */
        attr.send_cq = cq[0];
        attr.recv_cq = cq[0];
        peer->cq = cq[0];
    } else if (isServer) {
        /* Connection between server and client. */
	if (offset == 0) {
 		/* Each client will create two qps, we use same cq at server side. */
		cqPtr += 1;
		if (cqPtr >= cqNum)
			cqPtr = 1;
	}
        attr.send_cq = cq[cqPtr];
        attr.recv_cq = cq[cqPtr];
        peer->cq = cq[cqPtr];
    } else if (!isServer) {
        /* Client only have one CQ, so never change. */
        attr.send_cq = cq[cqPtr];
        attr.recv_cq = cq[cqPtr];
        peer->cq = cq[cqPtr];
        cqPtr += 1;
        if (cqPtr >= cqNum) {
            cqPtr = 0;
        } 
    }

    attr.cap.max_send_wr = QPS_MAX_DEPTH;
    attr.cap.max_recv_wr = QPS_MAX_DEPTH;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    peer->qp[offset] = ibv_create_qp(pd, &attr);
    Debug::notifyInfo("Create Queue Pair with Num = %d", peer->qp[offset]->qp_num);
    if (!peer->qp[offset]) {
    	Debug::notifyError("Failed to create QP");
    	return false;
    }
    return true;
}

bool RdmaSocket::ModifyQPtoInit(struct ibv_qp *qp) {
    if (qp == NULL) {
        Debug::notifyError("Bad QP, Return");
    }
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = Port;
    attr.pkey_index = 0;
    if (Mode == 0) {
        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    } else if (Mode == 1) {
        attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    }
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
    	Debug::notifyError("Failed to modify QP state to INIT");
    	return false;
    }
    return true;
}

bool RdmaSocket::ModifyQPtoRTR(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 3185;
    // attr.max_dest_rd_atomic = 1;
    // attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = Port;
    // if (GidIndex >= 0) {
    //     attr.ah_attr.is_global = 1;
    //     attr.ah_attr.port_num = 1;
    //     memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    //     attr.ah_attr.grh.flow_label = 0;
    //     attr.ah_attr.grh.hop_limit = 1;
    //     attr.ah_attr.grh.sgid_index = GidIndex;
    //     attr.ah_attr.grh.traffic_class = 0;
    // }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    // IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER
    if (Mode == 0) {
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12;
        flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    }
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
   		Debug::notifyError("failed to modify QP state to RTR");
   		return false;
    }
    return true;
}

bool RdmaSocket::ModifyQPtoRTS(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 3185;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    if (Mode == 0) {
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 16;
        attr.max_dest_rd_atomic = 16;
        flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    }
    // attr.max_rd_atomic = 1;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
    	Debug::notifyError("failed to modify QP state to RTS");
    	return false;
    }
    return true;
}

bool RdmaSocket::ConnectQueuePair(PeerSockData *peer) {
    if (MyNodeID == 2)
      Debug::debugItem("Debug-RdmaSocket.cpp: ConnectQueuePair exeuting");

    ExchangeMeta LocalMeta, RemoteMeta;
    GlexExchangeID LocalID, RemoteID;
	int rc = 0, N;
    bool ret;
	union ibv_gid MyGid;
    bool DoubleQP = false;
    if (isServer) {
        LocalID.NodeID = MyNodeID;
        LocalID.isServer = true;
        LocalID.GivenID = 0;
        if (isServer && MyNodeID == 1) {
            LocalID.GivenID = MaxNodeID;
        }
    } else {
        LocalID.NodeID = MyNodeID;
        LocalID.isServer = false;
        LocalID.GivenID = 0;
    }
    LocalID.nic_id = dev_attr.nic_id;
    LocalID.ep_num = ep_attr.num;

    /*------------  Change NodeID first -----------------------*/
    if (DataSyncwithSocket(peer->sock, sizeof(GlexExchangeID), (char *)&LocalID, (char *)&RemoteID) < 0) {
        Debug::notifyError("failed to exchange NodeID data between sides");
        rc = 1;
        goto ConnectQPExit;
    }
    if (MyNodeID == 2)
      Debug::debugItem("Debug-RdmaSocket.cpp: Change NodeID Done, local device nic_id is %d, eq num is %d. Remote device nic_id is %d, remote ep num is %d",
          (int)LocalID.nic_id, (int)LocalID.ep_num, (int)RemoteID.nic_id, (int)RemoteID.ep_num);
    if (isServer && RemoteID.isServer) {
        /* A server is connecting to me, we are both servers. */
        peer->NodeID = RemoteID.NodeID;
    } else if (isServer && !RemoteID.isServer && MyNodeID != 1) {
        peer->NodeID = RemoteID.NodeID;
    } else if (isServer && !RemoteID.isServer && MyNodeID == 1) {
        peer->NodeID = MaxNodeID;
        MaxNodeID += 1;
    } else if (!isServer && RemoteID.GivenID != 0) {
        MyNodeID = RemoteID.GivenID;
    }

    /*------------ Change registered memory handler----------------*/

    /*1. Compose remote ep address*/
    glex_ep_addr_t rmt_ep_addr;
    glex_ret_t Glex_ret;
    glex_compose_ep_addr(RemoteID.nic_id, RemoteID.ep_num, GLEX_EP_TYPE_NORMAL, &rmt_ep_addr);

    /*2. Init RDMA message request*/
    struct glex_imm_mp_req mp_req;
    struct glex_imm_mp_req *bad_mp_req;
    mp_req.rmt_ep_addr.v    = rmt_ep_addr.v;
    mp_req.data = &local_mh;
    mp_req.len              = sizeof(local_mh);
    mp_req.flag             = 0;
    mp_req.next             = NULL;

    /*3. Send rdma message*/
    Debug::debugItem("Debug-RdmaSocket.cpp: ready to send rdma message");
    while ((Glex_ret = glex_send_imm_mp(ep, &mp_req, &bad_mp_req)) == GLEX_BUSY) {
    }
    if (Glex_ret != GLEX_SUCCESS) {
        Debug::debugItem("Debug-RdmaSocket.cpp: Rdma message sent error");
        goto ConnectQPExit;
    } else {
        Debug::debugItem("Debug-RdmaSocket.cpp: Rdma message sent done");
    }

    /*4. Recieve rdma message*/
    Debug::debugItem("Debug-RdmaSocket.cpp: ready to recieve rdma message");
    uint64_t data[16];
    uint32_t msg_len;
    while ((Glex_ret = glex_receive_mp(ep, 0, &rmt_ep_addr, data, &msg_len)) == GLEX_NO_MP) {
    }
    if (Glex_ret != GLEX_SUCCESS) {
        Debug::debugItem("Debug-RdmaSocket.cpp: Rdma message recieve error");
        exit(1);
    } else {
        Debug::debugItem("Debug-RdmaSocket.cpp: Rdma message recieve done");
    }

    /*5. Construct remote memory handler*/
    glex_mem_handle_t rmt_mh;
    rmt_mh  = *(glex_mem_handle_t *)data;
    Debug::debugItem("Debug-RdmaSocket.cpp: rdma message exchanged, current node is %d", MyNodeID);

    /*6. Init peer data*/
    peer->rmt_ep_addr = rmt_ep_addr;
    peer->rmt_mh = rmt_mh;
    peer->rkey = 0x81;

    //CreateQueuePair(peer, 0);
    //CreateQueuePair(peer, 1);
    if (MyNodeID == 2)
      Debug::debugItem("Debug-RdmaSocket.cpp: CreateQueuePair Done");

    LocalMeta.rkey = 0x81;
    for (int i = 2; i < QP_NUMBER; i++)
      LocalMeta.qpNum[i] = 1;
    LocalMeta.lid = LocalID.ep_num;
    LocalMeta.RegisteredMemory = mm;
    memset(&MyGid, 0, sizeof(MyGid));
    memcpy(LocalMeta.gid, &MyGid, 16);
    if (DataSyncwithSocket(peer->sock, sizeof(ExchangeMeta), (char *)&LocalMeta, (char *)&RemoteMeta) < 0) {
                Debug::notifyError("failed to exchange connection data between sides");
        rc = 1;
        goto ConnectQPExit;
    }
    
    peer->lid = RemoteMeta.lid;
    peer->RegisteredMemory = RemoteMeta.RegisteredMemory;
    memcpy(peer->gid, RemoteMeta.gid, 16);
    N = 2;
    Debug::debugItem("Debug-RdmaSocket.cpp: ConnectQueuePair Done in server %d", MyNodeID);

        /*
        for (int i = 0; i < N; i++) {
        /* modify the QP to init 
        ret = ModifyQPtoInit(peer->qp[i]);
        if (ret == false)  {
            Debug::notifyError("change QP state to INIT failed");
            rc = 1;
            goto ConnectQPExit;
        }
        /* modify the QP to RTR 
        ret = ModifyQPtoRTR(peer->qp[i], peer->qpNum[i], peer->lid, peer->gid);
        if (ret == false) {
            Debug::notifyError("failed to modify QP state to RTR");
            rc = 1;
            goto ConnectQPExit;
        }
        /* Modify the QP to RTS 
        ret = ModifyQPtoRTS(peer->qp[i]);
        if (ret == false) {
            Debug::notifyError("failed to modify QP state to RTR");
            rc = 1;
            goto ConnectQPExit;
        }
    }
    */
    ConnectQPExit:
    if(rc != 0) {
    	return false;
    } else {
    	return true;
    }
}

int RdmaSocket::DataSyncwithSocket(int sock, int size, char *LocalData, char *RemoteData) {
    int rc;
    int readBytes = 0;
    int totalReadBytes = 0;
    rc = write(sock, LocalData, size);
    Debug::debugItem("Debug-RdmaSocket.cpp: DataSyncwithSocket write once, nodeid is %d, write bytes : %d, rc is %d", MyNodeID, size, rc);
    if (rc < size) {
    	Debug::notifyError("Failed writing data during sock_sync_data");
    } else {
    	rc = 0;
    }
    while (!rc && totalReadBytes < size) {
        readBytes = read(sock, RemoteData, size);
        Debug::debugItem("Debug-RdmaSocket.cpp: DataSyncwithSocket read once, %d bytes", readBytes);
        if (readBytes > 0) {
        	totalReadBytes += readBytes;
        } else {
        	rc = readBytes;
        }
    }
    return rc;
}

void RdmaSocket::SyncTool(uint16_t NodeID) {
    while (peers[NodeID] == NULL)
        usleep(100000);
    char bufferSend, bufferReceive;
    DataSyncwithSocket(peers[NodeID]->sock, 1, &bufferSend, &bufferReceive);
}

bool RdmaSocket::ResourcesDestroy() {

    glex_deregister_mem(ep, local_mh);
    printf("Deregister memory \n");
    glex_destroy_ep(ep);
    printf("Destroy ep\n");
    glex_close_device(dev);
    printf("Close glex device\n");

	bool rc = true;
    int i, j;
    for (i = 1; i <= ServerCount; i++) {
        if (peers[i] != NULL) {
            for (j = 0; j < QP_NUMBER; j++) {
                if (peers[i]->qp[j] != NULL) {
                    ibv_destroy_qp(peers[i]->qp[j]);
                    peers[i]->qp[j] = NULL;
                }
            }
        }
        free(peers[i]);
    }

    // for (i = 0; i < cqNum; i++) {
    //     if (cq[i]) {
    //         if (ibv_destroy_cq(cq[i])) {
    //                 Debug::notifyError("Failed to destroy CQ");
    //                 rc = 1;
    //             }
    //     }
    // }
    // free(cq);

    if (mr) {
        if (ibv_dereg_mr(mr)) {
            Debug::notifyError("Failed to deregister MR");
            rc = 1;
        }
    }

    if (pd) {
    	if (ibv_dealloc_pd(pd)) {
            Debug::notifyError("Failed to deallocate PD");
            rc = false;
        }
    }

    if (ctx) {
    	if (ibv_close_device(ctx)) {
            Debug::notifyError("failed to close device context");
            rc = false;
        }
    }
        
    return rc;
}

void RdmaSocket::RdmaListen() {
	struct sockaddr_in MyAddress;
	int sock;
	int on = 1;
	/* Socket Initialization */
	memset(&MyAddress,0,sizeof(MyAddress));
	MyAddress.sin_family=AF_INET;
	MyAddress.sin_addr.s_addr=INADDR_ANY;
	MyAddress.sin_port=htons(ServerPort);
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		Debug::debugItem("Socket creation failed");
	}

   	if ((setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) < 0) {
        Debug::debugItem("Setsockopt failed");
    }

	if (bind(sock, (struct sockaddr*)&MyAddress, sizeof(struct sockaddr)) < 0) {
		Debug::debugItem("Bind failed with errnum ", errno);
	}

	listen(sock,5);
	if (MyNodeID == 2)
  	  Debug::debugItem("Debug-RdmaSocket.cpp: RdmaListen, listen 5");

        Listener = thread(&RdmaSocket::RdmaAccept, this, sock);
        /* Connect to other servers. */
	if (MyNodeID == 2)
          Debug::debugItem("Debug-RdmaSocket.cpp: RdmaListen, Ready to ServerConnect");
        ServerConnect();
        if (MyNodeID == 2)
	  Debug::debugItem("Debug-RdmaSocket.cpp: RdmaListen, ServerConnect Done");

}

void RdmaSocket::RdmaAccept(int sock) {
    printf("Debug-RdmaSocket.cpp: RdmaAccept sock %d, nodeid is %d\n", sock, MyNodeID);
    struct sockaddr_in RemoteAddress;
    int fd;
    // struct timespec start, end;
    socklen_t sin_size = sizeof(struct sockaddr_in);
    while (isRunning && (fd = accept(sock, (struct sockaddr *)&RemoteAddress, &sin_size)) != -1)
    {
        Debug::notifyInfo("Discover New Client");
        PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
        peer->sock = fd;
        peer->counter = 0;
        if (ConnectQueuePair(peer) == false) {
            Debug::notifyError("RdmaAccept, RDMA connect with error");
        } else {
            peers[peer->NodeID] = peer;
            Debug::notifyInfo("Client %d Joined Us", peer->NodeID);
            /* Rdma Receive in Advance. */
            for (int i = 0; i < QPS_MAX_DEPTH; i++) {
                RdmaReceive(peer->NodeID, mm + peer->NodeID * 4096, 0);
            }

            Debug::debugItem("Accepted to Node%d", peer->NodeID);
        }
    }
}

void RdmaSocket::ServerConnect() {
    int sock;
    auto id2ip = conf->getInstance();
    for (auto &kv : id2ip) {
	Debug::debugItem("Debug-RdmaSocket.cpp: kv.first is %d, MyNodeID is %d",kv.first,  MyNodeID);
	/*original code use '<', don't know why*/
	//if (kv.first < MyNodeID) {
        if (kv.first != MyNodeID) {
	    Debug::debugItem("Debug-RdmaSocket.cpp: SocketConnect to %d, MyNodeID is %d",kv.first,  MyNodeID);
            sock = SocketConnect(kv.first);
            if (sock < 0) {
                Debug::notifyError("Socket connection failed to servers");
                return;
            }            PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
            peer->sock = sock;
            peer->NodeID = kv.first;

            Debug::debugItem("Debug-RdmaSocket.cpp: Ready to ConnectQueuePair, nodeid is %d", MyNodeID);

            if (ConnectQueuePair(peer) == false) {
                Debug::notifyError("ServerConnect, RDMA connect with error");
                return;
            } else {
		if (MyNodeID == 2)
	            Debug::debugItem("Debug-RdmaSocket.cpp: Set peer structure");
                //SyncTool(peer->NodeID);
                peers[peer->NodeID] = peer;
                peer->counter = 0;
		
                for (int i = 0; i < QPS_MAX_DEPTH; i++) {
                    RdmaReceive(peer->NodeID, mm + peer->NodeID * 4096, 0);
                }
		Debug::notifyError("Node %d finished Connecting to Node%d", MyNodeID, peer->NodeID);
                //Debug::debugItem("Finished Connecting to Node%d", peer->NodeID);
            }
        }
    }
}

int RdmaSocket::SocketConnect(uint16_t NodeID) {
	struct sockaddr_in RemoteAddress;
	int sock;
	struct timeval timeout = {3, 0};
	memset(&RemoteAddress, 0, sizeof(RemoteAddress));
	RemoteAddress.sin_family = AF_INET;
	inet_aton(conf->getIPbyID(NodeID).c_str(), (struct in_addr*)&RemoteAddress.sin_addr);
	RemoteAddress.sin_port = htons(ServerPort);
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		Debug::notifyError("Socket Creation Failed");
		return -1;
	}
	int ret = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
	if (ret < 0)
		Debug::notifyError("Set timeout failed!");

	int t = 3;
	while (t >= 0 && connect(sock, (struct sockaddr *)&RemoteAddress, sizeof(struct sockaddr)) < 0) {
		Debug::notifyError("Fail to connect to the server");
		t -= 1;
		usleep(1000000);
	}
	Debug::notifyError("Connect to the server %d", NodeID);
	if (t < 0) {
		return -1;
	}
	return sock;
}

void RdmaSocket::RdmaConnect() {
	int sock;
	/* Connect to Node 1 firstly to get clientID. */
	sock = SocketConnect(1);
	if(sock < 0) {
		Debug::notifyError("RdmaConnect, Socket connection failed to server 1");
		return;
	}
	PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
	peer->sock = sock;
	/* Add server's NodeID to the structure */
	peer->NodeID = 1;
	if (ConnectQueuePair(peer) == false) {
		Debug::notifyError("RdmaConnect, RDMA connect with error");
		return;
	} else {
		peers[peer->NodeID] = peer;
                peer->counter = 0;
                Debug::debugItem("RdmaConnect, Finished Connecting to Node%d", peer->NodeID);
	}
	/* Connect to other servers. */
	auto id2ip = conf->getInstance();
	for (auto &kv : id2ip) {
		if (kv.first != 1) {
			sock = SocketConnect(kv.first);
			if (sock < 0) {
				Debug::notifyError("RdmaConnect, Socket connection failed to servers");
				return;
			}
			PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
			peer->sock = sock;
			peer->NodeID = kv.first;
			if (ConnectQueuePair(peer) == false) {
				Debug::notifyError("RdmaConnect, RDMA connect with error");
				return;
			} else {
                //SyncTool(peer->NodeID);
				peers[peer->NodeID] = peer;
                peer->counter = 0;
                Debug::debugItem("RdmaConnect, Finished Connecting to Node%d", peer->NodeID);
			}
		}
	}
}
/*
* Only responsible for data transfer, memory copy is not maintained here.
* Assume that data has already been copied.
*/
bool RdmaSocket::RdmaSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize) {
    glex_ret_t tmpret;
    struct glex_imm_mp_req mp_req;
    struct glex_imm_mp_req *bad_mp_req;
    mp_req.rmt_ep_addr.v    = peers[NodeID]->rmt_ep_addr.v;
    mp_req.data = (void*)SourceBuffer;
    mp_req.len              = BufferSize;
    mp_req.flag             = 0;
    mp_req.next             = NULL;
    Debug::debugItem("Debug-RdmaSocket.cpp: RdmaSend ready");
    tmpret = glex_send_imm_mp(ep, &mp_req, &bad_mp_req);
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp: RdmaSend SUCCESS\n");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: RdmaSend GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: RdmaSend GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: RdmaSend Other error\n");
    }
    if(tmpret == GLEX_SUCCESS)
	return true;
    else
	return false;
    //assert(peers[NodeID]);
    /*
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
     
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.imm_data   = (uint32_t)MyNodeID;
    wr.opcode     = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(peers[NodeID]->qp[0], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_SEND failed.");
        return false;
    }
	return true;
    */
}

bool RdmaSocket::_RdmaBatchSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize) {
    assert(peers[NodeID]);
    PeerSockData *peer = peers[NodeID];

    glex_ret_t tmpret;
    struct glex_imm_mp_req mp_req[MAX_POST_LIST];
    struct glex_imm_mp_req *bad_mp_req;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        //if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
        //    PollCompletion(NodeID, 1, &wc);
        //}
	mp_req[w_i].rmt_ep_addr.v    = peer->rmt_ep_addr.v;
	mp_req[w_i].data = (void*)(SourceBuffer + w_i * 4096);
	mp_req[w_i].len = BufferSize;
	mp_req[w_i].flag = 0;
	mp_req[w_i].next = (w_i == BatchSize - 1) ? NULL : &mp_req[w_i + 1];
    }
    Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchSend ready");
    tmpret = glex_send_imm_mp(ep, &mp_req[0], &bad_mp_req);
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchSend SUCCESS");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchSend GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchSend GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: _RdmaBatchSend Other error\n");
    }
    if(tmpret == GLEX_SUCCESS)
        return true;
    else
        return false;
    /*
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 4096;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id      = 0;
        send_wr[w_i].imm_data   = (uint32_t)MyNodeID;
        send_wr[w_i].opcode     = IBV_WR_SEND_WITH_IMM;
        send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        //send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        peer->counter += 1;
    }
    if (ibv_post_send(peer->qp[0], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_SEND failed.");
        return false;
    }
    return true;
    */
}

bool RdmaSocket::RdmaReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize) {
    uint32_t msg_len = BufferSize;
    Debug::debugItem("Debug-RdmaSocket.cpp: RdmaReceive ready");
    glex_ret_t tmpret;
    tmpret = glex_receive_mp(ep, 0, &(peers[NodeID]->rmt_ep_addr), (void*)SourceBuffer, &msg_len);
    switch (tmpret) {
	case GLEX_SUCCESS :
	    Debug::debugItem("Debug-RdmaSocket.cpp: RdmaReceive SUCCESS");
	    return true;
	    break;
	case GLEX_NO_MP :
	    Debug::debugItem("Debug-RdmaSocket.cpp: RdmaReceive GLEX_NO_MP");
	    break;
	case GLEX_INVALID_PARAM :
	    printf("Debug-RdmaSocket.cpp: RdmaReceive GLEX_INVALID_PARAM\n");
	    break;
	default:
	   printf("Debug-RdmaSocket.cpp: RdmaReceive Other error\n");
    }
    return false;
    //assert(peers[NodeID]);
    /*
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *wrBad;
    int ret; 
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    ret = ibv_post_recv(peers[NodeID]->qp[0], &wr, &wrBad);
    if (ret) {
        Debug::notifyError("Receive with RDMA_RECV failed, ret = %d.", ret);
        return false;
    }
	return true;
   */
}

bool RdmaSocket::_RdmaBatchReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize) {
    uint32_t msg_len = BufferSize;
    Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchReceive ready");
    glex_ret_t tmpret;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
      tmpret = glex_receive_mp(ep, 0, &(peers[NodeID]->rmt_ep_addr), (void*)(SourceBuffer + w_i * 4096), &msg_len);
    }
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp:_RdmaBatchReceive SUCCESS");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp:_RdmaBatchReceive GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchReceive GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp:_RdmaBatchReceive Other error\n");
    }
    if (tmpret == GLEX_SUCCESS)
        return true;
    else
	return false;
    /*
    struct ibv_recv_wr recv_wr[MAX_POST_LIST], *bad_recv_wr;
    struct ibv_sge sgl[MAX_POST_LIST];
    int w_i;
    int ret;
    for(w_i = 0; w_i < BatchSize; w_i++) {
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey = mr->lkey;
        sgl[w_i].addr = (uintptr_t)SourceBuffer + w_i * 4096;
        recv_wr[w_i].sg_list = &sgl[w_i];
        recv_wr[w_i].num_sge = 1;
        recv_wr[w_i].next = (w_i == BatchSize - 1) ? NULL : &recv_wr[w_i + 1];
    }
    ret = ibv_post_recv(peers[NodeID]->qp[0], &recv_wr[0], &bad_recv_wr);
    if (ret) {
	Debug::notifyError("Receive with RDMA_RECV failed, ret = %d.", ret);
	return false;
    }
    return true;
    */
}

bool RdmaSocket::RdmaRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int TaskID, bool localEvent) {
    //assert(peers[NodeID]);
    struct glex_rdma_req rdma_req;
    struct glex_rdma_req *bad_rdma_req;
    glex_ret_t tmpret;
    rdma_req.rmt_ep_addr.v  = peers[NodeID]->rmt_ep_addr.v;
    rdma_req.local_mh.v     = local_mh.v;
    rdma_req.type           = GLEX_RDMA_TYPE_GET;
    rdma_req.rmt_key        = 0X81;
    rdma_req.flag           = 0;
    if (localEvent) {
	/*Indicate that RDMA read operation has finished*/
	rdma_req.flag           = GLEX_FLAG_LOCAL_EVT;
	rdma_req.local_evt.cookie_0 = 0x9696969600000000ULL;
	rdma_req.local_evt.cookie_1 = 0x9696969600000000ULL;
    }
    rdma_req.next           = NULL;
    rdma_req.local_offset   = SourceBuffer - mm;
    rdma_req.len            = BufferSize;
    rdma_req.rmt_mh.v       = peers[NodeID]->rmt_mh.v;
    rdma_req.rmt_offset     = DesBuffer;
    Debug::debugItem("Debug-RdmaSocket.cpp: RdmaRead ready");
    while ((tmpret = glex_rdma(ep, &rdma_req, &bad_rdma_req)) == GLEX_BUSY) {
    }
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp: RdmaRead SUCCESS");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: RdmaRead GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: RdmaRead GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: RdmaRead Other error\n");
    }
    if (tmpret == GLEX_SUCCESS)
        return true;
    else
        return false;
    /*
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
     
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = DesBuffer + peers[NodeID]->RegisteredMemory;
    wr.wr.rdma.rkey        = peers[NodeID]->rkey;
     
    if (ibv_post_send(peers[NodeID]->qp[TaskID], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_READ failed.");
        return false;
    }
	return true;
    */
}

bool RdmaSocket::_RdmaBatchRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int BatchSize) {
    //assert(peers[NodeID]);

    struct glex_rdma_req rdma_req[MAX_POST_LIST];
    struct glex_rdma_req *bad_rdma_req;
    glex_ret_t tmpret;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
 	rdma_req[w_i].rmt_ep_addr.v  = peers[NodeID]->rmt_ep_addr.v;
        rdma_req[w_i].local_mh.v     = local_mh.v;
        rdma_req[w_i].type           = GLEX_RDMA_TYPE_GET;
        rdma_req[w_i].rmt_mh.v       = peers[NodeID]->rmt_mh.v;
        rdma_req[w_i].rmt_key        = 0X81;
        rdma_req[w_i].flag           = 0;

        rdma_req[w_i].local_offset   = SourceBuffer + w_i * 4096 - mm; /*SourceBuffer + w_i * 8 - mm*/
        rdma_req[w_i].len            = BufferSize;
        rdma_req[w_i].rmt_offset     = DesBuffer + w_i * 4096;
        rdma_req[w_i].next           = (w_i == BatchSize - 1) ? NULL : &rdma_req[w_i + 1];
    }

    printf("Debug-RdmaSocket.cpp: _RdmaBatchRead ready");
    while ((tmpret = glex_rdma(ep, &rdma_req[0], &bad_rdma_req)) == GLEX_BUSY) {
    }
    switch (tmpret) {
        case GLEX_SUCCESS :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchRead SUCCESS\n");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchRead GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchRead GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: _RdmaBatchRead Other error\n");
    }
    if (tmpret == GLEX_SUCCESS)
        return true;
    else
        return false;

    /*
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    struct ibv_wc wc;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 8;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id      = 0;
        send_wr[w_i].opcode     = IBV_WR_RDMA_READ;
        send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        send_wr[w_i].wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory;// + w_i * 4096;
        send_wr[w_i].wr.rdma.rkey        = peer->rkey;
        peer->counter += 1;
    }

    if (ibv_post_send(peer->qp[0], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_READ failed.");
        return false;
    }
    return true;
    */
}

bool RdmaSocket::RemoteRead(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    int shipSize;
    TransferTask tasks[4];
    if (size < 4 * 1024 * 1024) {
        /* Small size read, no need to use multithread to transfer. */
        InboundHamal(0, bufferSend, NodeID, bufferReceive, size);
        return true;
    } else {
        /* Each thread transfer one part, 4-KB Aligned. */
        TransferSignal = 0;
        shipSize = size / WORKER_NUMBER;
        shipSize = shipSize >> 12 << 12;
        for (int i = 0; i < WORKER_NUMBER; i++) {
            tasks[i].OpType = false;
            tasks[i].size = (i < 3) ? shipSize : (size - 3 * shipSize);
            tasks[i].bufferReceive = bufferReceive + i * shipSize;
            tasks[i].NodeID = NodeID;
            tasks[i].bufferSend = bufferSend + i * shipSize;
            queue[i].PushPolling(&tasks[i]);
        }
        while (TransferSignal != WORKER_NUMBER);
        return true;
    }
}

bool RdmaSocket::DataTransferWorker(int id) {
    TransferTask *task;
    while (true) {
        task = queue[id].PopPolling();
        if (task->OpType) {
            /* Write opration. */
            OutboundHamal(id, task->bufferSend, task->NodeID, task->bufferReceive, task->size);
        } else {
            InboundHamal(id, task->bufferSend, task->NodeID, task->bufferReceive, task->size);
        }
    }
}

bool RdmaSocket::InboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    uint64_t SendPoolSize = 1024 * 1024;
    uint64_t SendPoolAddr = mm + 4 * 1024 + TaskID * 1024 * 1024;
    uint64_t TotalSizeSend = 0; 
    uint64_t SendSize;
    struct ibv_wc wc;
    struct  timeval start, end;
    uint64_t diff;
    while (TotalSizeSend < size) {
        SendSize = (size - TotalSizeSend) >= SendPoolSize ? SendPoolSize : (size - TotalSizeSend);
        // _RdmaBatchRead(NodeID, 
        //                SendPoolAddr, 
        //                bufferReceive + TotalSizeSend, 
        //                SendSize, 
        //                1);
        gettimeofday(&start, NULL);
        RdmaRead(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, SendSize, TaskID + 1, true);
        PollCompletion(NodeID, 1, &wc);
        memcpy((void *)(bufferSend + TotalSizeSend), (void *)SendPoolAddr, SendSize);
        gettimeofday(&end, NULL);
        diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
        ReadSize[TaskID] += SendSize;
        ReadTimeCost[TaskID] += diff;
        TotalSizeSend += SendSize;
    }
    __sync_fetch_and_add( &TransferSignal, 1 );
    return true;
}

bool RdmaSocket::RdmaWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int TaskID) {
    //assert(peers[NodeID]);
    if (((int32_t)imm == -1) || (imm == -2)) {
        struct glex_rdma_req rdma_req;
        struct glex_rdma_req *bad_rdma_req;
        glex_ret_t tmpret;
        rdma_req.rmt_ep_addr.v  = peers[NodeID]->rmt_ep_addr.v;
        rdma_req.local_mh.v     = local_mh.v;
        rdma_req.type           = GLEX_RDMA_TYPE_PUT;
        rdma_req.rmt_key        = 0X81;
        rdma_req.flag           = 0;
	if (imm == -2) {
		rdma_req.flag           = GLEX_FLAG_LOCAL_EVT;

	        /*Remote Event*/
	        rdma_req.local_evt.cookie_0 = 0x9696969600000000ULL; /*Indicate that RDMA write operation has finished*/
        	rdma_req.local_evt.cookie_1 = 0x9696969600000000ULL;
	}
        rdma_req.next           = NULL;
        rdma_req.local_offset   = SourceBuffer - mm;
        rdma_req.len            = BufferSize;
        rdma_req.rmt_mh.v       = peers[NodeID]->rmt_mh.v;
        rdma_req.rmt_offset     = DesBuffer;
        Debug::debugItem("Debug-RdmaSocket.cpp: RdmaWrite ready");
        while ((tmpret = glex_rdma(ep, &rdma_req, &bad_rdma_req)) == GLEX_BUSY) {
        }
        switch (tmpret) {
            case GLEX_SUCCESS :
                Debug::debugItem("Debug-RdmaSocket.cpp: RdmaWrite SUCCESS");
                break;
            case GLEX_NO_MP :
                printf("Debug-RdmaSocket.cpp: RdmaWrite GLEX_NO_MP\n");
                break;
            case GLEX_INVALID_PARAM :
                printf("Debug-RdmaSocket.cpp: RdmaWrite GLEX_INVALID_PARAM\n");
                break;
            default:
               printf("Debug-RdmaSocket.cpp: RdmaWrite Other error\n");
        }
        if (tmpret == GLEX_SUCCESS)
            return true;
        else
            return false;

    } else {
      Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) faild");
    }
    /*
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    if((int32_t)imm == -1) {
        wr.opcode     = IBV_WR_RDMA_WRITE;
    } else {
        wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.imm_data   = imm;
    }
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory;
    Debug::debugItem("Post RDMA_WRITE with remote address = %lx", wr.wr.rdma.remote_addr);
    wr.wr.rdma.rkey        = peer->rkey;
    if (ibv_post_send(peer->qp[TaskID], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
        printf("%s\n", strerror(errno));
        return false;
    }
	return true;
    */
}

bool RdmaSocket::RemoteWrite(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    int shipSize;
    TransferTask tasks[4];
    if (size < 4 * 1024 * 1024) {
        /* Small size write, no need to use multithread to transfer. */
        OutboundHamal(0, bufferSend, NodeID, bufferReceive, size);
        return true;
    }  else {
        /* Each thread transfer one part, 4-KB Aligned. */
        TransferSignal = 0;
        shipSize = size / WORKER_NUMBER;
        shipSize = shipSize >> 12 << 12;
        for (int i = 0; i < WORKER_NUMBER; i++) {
            tasks[i].OpType = true;
            tasks[i].size = (i < 3) ? shipSize : (size - 3 * shipSize);
            tasks[i].bufferReceive = bufferReceive + i * shipSize;
            tasks[i].NodeID = NodeID;
            tasks[i].bufferSend = bufferSend + i * shipSize;
            queue[i].PushPolling(&tasks[i]);
        }
        while (TransferSignal != WORKER_NUMBER);
        return true;
    }
}

bool RdmaSocket::OutboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    uint64_t SendPoolSize = 1024 * 1024;
    uint64_t SendPoolAddr = mm + 4 * 1024 + TaskID * 1024 * 1024;
    uint64_t TotalSizeSend = 0; 
    uint64_t SendSize;
    struct ibv_wc wc;
    struct  timeval start, end;
    uint64_t diff;
    while (TotalSizeSend < size) {
        SendSize = (size - TotalSizeSend) >= SendPoolSize ? SendPoolSize : (size - TotalSizeSend);
        gettimeofday(&start,NULL);
        memcpy((void *)SendPoolAddr, (void *)(bufferSend + TotalSizeSend), SendSize);
        // _RdmaBatchWrite(NodeID, 
        //                SendPoolAddr, 
        //                bufferReceive + TotalSizeSend, 
        //                SendSize, 
        //                (uint32_t)-1,
        //                1);
        RdmaWrite(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, SendSize, -2, TaskID + 1);
        PollCompletion(NodeID, 1, &wc);
        // if (SendSize > 32 * 1024) {
        //     /* Wait Until write finish, May help. */
        //     RdmaRead(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, 1);
        //     PollCompletion(NodeID, 1, &wc);
        // }
        gettimeofday(&end,NULL);
        diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
        WriteSize[TaskID] += SendSize;
        WriteTimeCost[TaskID] += diff;
        /* RdmaWrite Testing. */
        if (WriteTest) {
            gettimeofday(&start,NULL);
            for (int i = 0; i < 10; i ++) {
                RdmaWrite(NodeID, SendPoolAddr, bufferReceive, 1024 * 1024, -2, TaskID + 1);
                PollCompletion(NodeID, 1, &wc);
            }
            gettimeofday(&end,NULL);
            diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
            printf("diff = %d, size = 10MB.\n", (int)diff);
            WriteTest = false;
        }
        Debug::debugItem("Source Addr = %lx, Des Addr = %lx, Size = %d", SendPoolAddr, bufferReceive + TotalSizeSend, SendSize);
        TotalSizeSend += SendSize;
    }
    __sync_fetch_and_add( &TransferSignal, 1 );
    return true;
}

bool RdmaSocket::_RdmaBatchWriteGlex(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize,  int BatchSize, uint64_t imm_nodeid, uint64_t imm_offset) {
    struct glex_rdma_req rdma_req[MAX_POST_LIST];
    struct glex_rdma_req *bad_rdma_req;
    glex_ret_t tmpret;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        rdma_req[w_i].rmt_ep_addr.v  = peers[NodeID]->rmt_ep_addr.v;
        rdma_req[w_i].local_mh.v     = local_mh.v;
        rdma_req[w_i].type           = GLEX_RDMA_TYPE_PUT;
        rdma_req[w_i].rmt_mh.v       = peers[NodeID]->rmt_mh.v;
        rdma_req[w_i].rmt_key        = 0X81;
        rdma_req[w_i].flag           = GLEX_FLAG_REMOTE_EVT;

        /*Remote Event*/
        rdma_req[w_i].rmt_evt.cookie_0 = imm_offset;
        rdma_req[w_i].rmt_evt.cookie_1 = imm_nodeid;

        rdma_req[w_i].local_offset   = SourceBuffer + w_i * 4096 - mm; /*SourceBuffer + w_i * 8 - mm*/
        rdma_req[w_i].len            = BufferSize;
        rdma_req[w_i].rmt_offset     = DesBuffer + w_i * 4096;
        rdma_req[w_i].next           = (w_i == BatchSize - 1) ? NULL : &rdma_req[w_i + 1];
    }

    Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchWriteGlex ready");
    while ((tmpret = glex_rdma(ep, &rdma_req[0], &bad_rdma_req)) == GLEX_BUSY) {
    }
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchWriteGlex SUCCESS");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchWriteGlex GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchWriteGlex GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: _RdmaBatchWriteGlex Other error\n");
    }
    //PollCompletionGlex();
    if (tmpret == GLEX_SUCCESS)
        return true;
    else
        return false;
}

bool RdmaSocket::_RdmaBatchWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int BatchSize) {
    //assert(peers[NodeID]);
    /*
    if (imm != 0) {
	Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
        return false;
    }
    */
    struct glex_rdma_req rdma_req[MAX_POST_LIST];
    struct glex_rdma_req *bad_rdma_req;
    glex_ret_t tmpret;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        rdma_req[w_i].rmt_ep_addr.v  = peers[NodeID]->rmt_ep_addr.v;
        rdma_req[w_i].local_mh.v     = local_mh.v;
        rdma_req[w_i].type           = GLEX_RDMA_TYPE_PUT;
        rdma_req[w_i].rmt_mh.v       = peers[NodeID]->rmt_mh.v;
        rdma_req[w_i].rmt_key        = 0X81;
	rdma_req[w_i].flag           = 0;

	if ((int32_t)imm != 0) {
	        rdma_req[w_i].flag           = GLEX_FLAG_REMOTE_EVT;
		/*Remote Event*/
	        rdma_req[w_i].rmt_evt.cookie_1 = 0xa5a5a5a55a5a5a5aULL;
	}



        rdma_req[w_i].local_offset   = SourceBuffer + w_i * 4096 - mm; /*SourceBuffer + w_i * 8 - mm*/
        rdma_req[w_i].len            = BufferSize;
        rdma_req[w_i].rmt_offset     = DesBuffer + w_i * 4096;
        rdma_req[w_i].next           = (w_i == BatchSize - 1) ? NULL : &rdma_req[w_i + 1];
    }

    Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchWrite ready");
    while ((tmpret = glex_rdma(ep, &rdma_req[0], &bad_rdma_req)) == GLEX_BUSY) {
    }
    switch (tmpret) {
        case GLEX_SUCCESS :
            Debug::debugItem("Debug-RdmaSocket.cpp: _RdmaBatchWrite SUCCESS");
            break;
        case GLEX_NO_MP :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchWrite GLEX_NO_MP\n");
            break;
        case GLEX_INVALID_PARAM :
            printf("Debug-RdmaSocket.cpp: _RdmaBatchWrite GLEX_INVALID_PARAM\n");
            break;
        default:
           printf("Debug-RdmaSocket.cpp: _RdmaBatchWrite Other error\n");
    }
    if (tmpret == GLEX_SUCCESS)
        return true;
    else
        return false;
    /*
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    struct ibv_wc wc;
    int w_i;
    //printf("NodeID = %d, qp_num = %lx, cq = %lx, rkey = %x\n", NodeID, peer->qp->qp_num, peer->cq, peer->rkey);
    for (w_i = 0; w_i < BatchSize; w_i++) {
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 4096;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        if ((int32_t)imm == 0) {
             send_wr[w_i].opcode = IBV_WR_RDMA_WRITE;
         } else {
             send_wr[w_i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
             send_wr[w_i].imm_data = imm;
         }
        send_wr[w_i].wr_id      = 0;
        
        send_wr[w_i].send_flags = 0;
        send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        
        //send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        send_wr[w_i].wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory + w_i * 4096;
        Debug::debugItem("remote address = %lx, Counter = %d, imm = %lx", send_wr[w_i].wr.rdma.remote_addr, peer->counter, imm);
        send_wr[w_i].wr.rdma.rkey        = peer->rkey;
        peer->counter += 1;
    }

    if (ibv_post_send(peer->qp[0], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
        printf("%s\n", strerror(errno));
        return false;
    }
    return true;
    */
}

bool RdmaSocket::RdmaFetchAndAdd(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Add) {
    //assert(peers[NodeID]);
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = 8;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = DesBuffer + peer->RegisteredMemory;
    wr.wr.atomic.rkey        = peer->rkey;
    wr.wr.atomic.compare_add = Add; /* value to be added to the remote address content */
     
    if (ibv_post_send(peer->qp[0], &wr, &wrBad)) {
        Debug::notifyError("Send with ATOMIC_FETCH_AND_ADD failed.");
        return false;
    }
	return true;
}

bool RdmaSocket::RdmaCompareAndSwap(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Compare, uint64_t Swap) {
    //assert(peers[NodeID]);
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = 8;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = DesBuffer + peer->RegisteredMemory;
    wr.wr.atomic.rkey        = peer->rkey;
    wr.wr.atomic.compare_add = Compare; /* expected value in remote address */
    wr.wr.atomic.swap        = Swap; /* the value that remote address will be assigned to */
     
    if (ibv_post_send(peer->qp[0], &wr, &wrBad)) {
        Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
        return false;
    }
    return true;
}

int RdmaSocket::PollCompletionGlex() {
    int count = 0;
    glex_event_t *event;
    Debug::debugItem("Debug-RdmaSocket.cpp: PollCompletion ready");
    while (glex_probe_next_event(ep, &event) == GLEX_NO_EVENT){
    }
    glex_discard_probed_event(ep);
    Debug::debugItem("Debug-RdmaSocket.cpp: PollCompletion Done");
}

int RdmaSocket::PollCompletion(uint16_t NodeID, int PollNumber, struct ibv_wc *wc) {
    int count = 0;
    glex_event_t *event;
    Debug::debugItem("Debug-RdmaSocket.cpp: PollCompletion ready");
    while (glex_probe_next_event(ep, &event) == GLEX_NO_EVENT){
    }
    glex_discard_probed_event(ep);
    Debug::debugItem("Debug-RdmaSocket.cpp: PollCompletion Done");
    /*
    do {
        count += ibv_poll_cq(peers[NodeID]->cq, 1, wc);
    } while (count < PollNumber);
     
    if (count < 0) {
        Debug::notifyError("Poll Completion failed.");
        return -1;
    }
     
    /* Check Completion Status
    if (wc->status != IBV_WC_SUCCESS) {
        Debug::notifyError("Failed status %s (%d) for wr_id %d", 
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    }
    Debug::debugItem("Find New Completion Message");
    return count;
    */
}

int RdmaSocket::PollWithCQ(int cqPtr, int PollNumber, struct ibv_wc *wc) {
    int count = 0;
     
    do {
        count += ibv_poll_cq(cq[cqPtr], 1, wc);
    } while (count < PollNumber);

    if (count < 0) {
        Debug::notifyError("Poll Completion failed.");
        return -1;
    }
    
    /* Check Completion Status */
    if (wc->status != IBV_WC_SUCCESS) {
        Debug::notifyError("Failed status %s (%d) for wr_id %d", 
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    }
    Debug::debugItem("Find New Completion Message");
    return count;
}

bool RdmaSocket::GlexPollOnce(uint16_t *NodeID, uint16_t *offset) {
    glex_ret_t ret;
    struct glex_event *event;
    ret = glex_probe_next_event(ep, &event);
    if (ret == GLEX_SUCCESS) {
	//glex_discard_probed_event(ep);
	*NodeID = (uint16_t)event->cookie_1;
	*offset = (uint16_t)event->cookie_0;
	Debug::debugItem("Debug-RdmaSocket.cpp: GlexPollOnce, Probe event once,  NodeID is %d, offset is %d", (uint16_t)event->cookie_1, (uint16_t)event->cookie_0);
	return true;
    } else if (ret == GLEX_NO_EVENT) {
	//printf("Debug-RdmaSocket.cpp: GlexPollOnce, on event found\n");
      return false;
    }
    return false;
}

void RdmaSocket::GlexDuscardOnce() {
	glex_discard_probed_event(ep);
}

int RdmaSocket::PollOnce(int cqPtr, int PollNumber, struct ibv_wc *wc) {
    int count = ibv_poll_cq(cq[cqPtr], PollNumber, wc);
    if (count == 0) {
        return 0;
    } else if (count < 0) {
	Debug::notifyError("Failure occurred when reading work completions, ret = %d", count);
	return 0;
    }
    if (wc->status != IBV_WC_SUCCESS) {
	Debug::notifyError("Failed status %s (%d) for wr_id %d",
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    } else {
        return count;
    }
}

int RdmaSocket::getCQCount() {
    return cqNum;
}

uint16_t RdmaSocket::getNodeID() {
    return MyNodeID;
}
void RdmaSocket::WaitClientConnection(uint16_t NodeID) {
    while(peers[NodeID] == NULL)
        usleep(1);
}

PeerSockData* RdmaSocket::getPeerInformation(uint16_t NodeID) {
    return peers[NodeID];
}

void RdmaSocket::RdmaQueryQueuePair(uint16_t NodeID) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(peers[NodeID]->qp[0], &attr, IBV_QP_STATE, &init_attr);
    switch (attr.qp_state) {
        case IBV_QPS_RESET:
            printf("Client %d with QP state: IBV_QPS_RESET\n", NodeID);
            break;
        case IBV_QPS_INIT:
            printf("Client %d with QP state: IBV_QPS_INIT\n", NodeID);
            break;
        case IBV_QPS_RTR:
            printf("Client %d with QP state: IBV_QPS_RTR\n", NodeID);
            break;
        case IBV_QPS_RTS:
            printf("Client %d with QP state: IBV_QPS_RTS\n", NodeID);
            break;
        case IBV_QPS_SQD:
            printf("Client %d with QP state: IBV_QPS_SQD\n", NodeID);
            break;
        case IBV_QPS_SQE:
            printf("Client %d with QP state: IBV_QPS_SQE\n", NodeID);
            break;
        case IBV_QPS_ERR:
            printf("Client %d with QP state: IBV_QPS_ERR\n", NodeID);
            break;
        case IBV_QPS_UNKNOWN:
            printf("Client %d with QP state: IBV_QPS_UNKNOWN\n", NodeID);
            break;
    }
}
