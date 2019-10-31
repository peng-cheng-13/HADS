/*** File system class. ***/

/** Version 2 + modifications for functional model. **/

/** Included files. **/
#include "filesystem.hpp"
#include "RPCServer.hpp"
extern RPCServer *server;
bool Dotx = true;
uint64_t TxLocalBegin() {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->TxLocalBegin();
}

void TxWriteData(uint64_t TxID, uint64_t address, uint64_t size) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxWriteData(TxID, address, size);
}

uint64_t getTxWriteDataAddress(uint64_t TxID) {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->getTxWriteDataAddress(TxID);
}

void TxLocalCommit(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxLocalCommit(TxID, action);
}

uint64_t TxDistributedBegin() {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->TxDistributedBegin();
}

void TxDistributedPrepare(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxDistributedPrepare(TxID, action);
}

void TxDistributedCommit(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxDistributedCommit(TxID, action);
}

void FileSystem::updateRemoteMeta(uint16_t parentNodeID, DirectoryMeta *meta, uint64_t parentMetaAddress, uint64_t parentHashAddress) {
	Debug::debugTitle("updateRemoteMeta");
	/* Prepare imm data. */
	uint32_t imm, temp;
	/*
	|  12b  |     20b    |
	+-------+------------+
	| 0XFFF | HashAdress |
	+-------+------------+
	*/
	temp = 0XFFF;
	imm = (temp << 20);
	imm += (uint32_t)parentHashAddress;
	/* Remote write with imm. */
	uint64_t SendBuffer;
	server->getMemoryManagerInstance()->getServerSendAddress(parentNodeID, &SendBuffer);
	uint64_t RemoteBuffer = parentMetaAddress;
	Debug::debugItem("imm = %x, SendBuffer = %lx, RemoteBuffer = %lx", imm, SendBuffer, RemoteBuffer);
	if (parentNodeID == server->getRdmaSocketInstance()->getNodeID()) {
		Debug::debugItem("updateMeta, memcopy to local node");
		memcpy((void *)(RemoteBuffer + server->getMemoryManagerInstance()->getDmfsBaseAddress()), (void *)meta, sizeof(DirectoryMeta));
		//unlockWriteHashItem(0, parentNodeID, parentHashAddress);
		return;
	}
	Debug::debugItem("updateMeta, RDMA wirte to remotenode");
        uint64_t size = sizeof(DirectoryMeta) - sizeof(DirectoryMetaTuple) * (MAX_DIRECTORY_COUNT - meta->count);
	memcpy((void *)SendBuffer, (void *)meta, size);
        server->getRdmaSocketInstance()->RdmaWrite(parentNodeID, SendBuffer, RemoteBuffer, size, -1, 1);
	server->getRdmaSocketInstance()->RdmaRead(parentNodeID, SendBuffer, RemoteBuffer, size, 1, false);
	/* Data will be written to the remote address, and lock will be released with the assist of imm data. */
	/* WRITE READ will be send after that, flushing remote data. */
}

void RdmaCall(uint16_t NodeID, char *bufferSend, uint64_t lengthSend, char *bufferReceive, uint64_t lengthReceive) {
    server->getRPCClientInstance()->RdmaCall(NodeID, bufferSend, lengthSend, bufferReceive, lengthReceive);
}

/** Implemented functions. **/ 
/* Check if node hash is local. 
   @param   hashNode    Given node hash.
   @return              If given node hash points to local node return true, otherwise return false. */
bool FileSystem::checkLocal(NodeHash hashNode)
{
    if (hashNode == hashLocalNode) {
        return true;                    /* Succeed. Local node. */
    } else {
        return false;                   /* Succeed. Remote node. */
    }
}

/* Get parent directory. 
   Examples:    "/parent/file" -> "/parent" return true
                "/file"        -> "/" return true
                "/"            -> return false
   @param   path    Path.
   @param   parent  Buffer to hold parent path.
   @return          If succeed return true, otherwise return false. */
bool FileSystem::getParentDirectory(const char *path, char *parent) { /* Assume path is valid. */
    if ((path == NULL) || (parent == NULL)) { /* Actually there is no need to check. */
        return false;
    } else {
        strcpy(parent, path);           /* Copy path to parent buffer. Though it is better to use strncpy later but current method is simpler. */
        uint64_t lengthPath = strlen(path); /* FIXME: Might cause memory leak. */
        if ((lengthPath == 1) && (path[0] == '/')) { /* Actually there is no need to check '/' if path is assumed valid. */
            return false;               /* Fail due to root directory has no parent. */
        } else {
            bool resultCut = false;
            for (int i = lengthPath - 1; i >= 0; i--) { /* Limit in range of signed integer. */
                if (path[i] == '/') {
                    parent[i] = '\0';   /* Cut string. */
                    resultCut = true;
                    break;
                } 
            }
            if (resultCut == false) {
                return false;           /* There is no '/' in string. It is an extra check. */
            } else {
                if (parent[0] == '\0') { /* If format is '/path' which contains only one '/' then parent is '/'. */
                    parent[0] = '/';
                    parent[1] = '\0';
                    return true;        /* Succeed. Parent is root directory. */ 
                } else {
                    return true;        /* Succeed. */
                }
            }
        }
    }
}

/* Get file name from path. 
   Examples:    '/parent/file' -> 'file' return true
                '/file'        -> 'file' return true
                '/'            -> return false
   @param   path    Path.
   @param   name    Buffer to hold file name.
   @return          If succeed return true, otherwise return false. */
bool FileSystem::getNameFromPath(const char *path, char *name) { /* Assume path is valid. */
    if ((path == NULL) || (name == NULL)) { /* Actually there is no need to check. */
        return false;
    } else {
        uint64_t lengthPath = strlen(path); /* FIXME: Might cause memory leak. */
        if ((lengthPath == 1) && (path[0] == '/')) { /* Actually there is no need to check '/' if path is assumed valid. */
            return false;               /* Fail due to root directory has no parent. */
        } else {
            bool resultCut = false;
            int i;
            for (i = lengthPath - 1; i >= 0; i--) { /* Limit in range of signed integer. */
                if (path[i] == '/') {
                    resultCut = true;
                    break;
                } 
            }
            if (resultCut == false) {
                return false;           /* There is no '/' in string. It is an extra check. */
            } else {
                strcpy(name, &path[i + 1]); /* Copy name to name buffer. path[i] == '/'. Though it is better to use strncpy later but current method is simpler. */
                return true;
            }
        }
    }
}

/* Lock hash item for write. */
uint64_t FileSystem::lockWriteHashItem(NodeHash hashNode, AddressHash hashAddress)
{
    //NodeHash hashNode = storage->getNodeHash(hashUnique); /* Get node hash. */
    //AddressHash hashAddress = hashUnique->value[0] & 0x00000000000FFFFF; /* Get address hash. */
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before write lock: %lx", *value);
    uint64_t ret = lock->WriteLock((uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after write lock, address: %lx, key: %lx", value, ret);
    return ret;
}

/* Unlock hash item. */
void FileSystem::unlockWriteHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before write unlock: %lx", *value);
    lock->WriteUnlock(key, (uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after write unlock: %lx address = %lx", value, *value);
}

/* Lock hash item for read. */
uint64_t FileSystem::lockReadHashItem(NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before read lock: %lx", *value);
    uint64_t ret = lock->ReadLock((uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after read lock, address: %lx, key: %lx", *value, ret);
    return ret;
}
/* Unlock hash item. */
void FileSystem::unlockReadHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before read unlock: %lx", *value);
    lock->ReadUnlock(key, (uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after read unlock: %lx", *value);
}

/* Send message. */
bool FileSystem::sendMessage(NodeHash hashNode, void *bufferSend, uint64_t lengthSend, 
    void *bufferReceive, uint64_t lengthReceive)
{
    return true;//return _cmd.sendMessage((uint16_t)hashNode, (char *)bufferSend, lengthSend, (char *)bufferReceive, lengthReceive); /* Actual send message. */
}

/* Parse message. */
void FileSystem::parseMessage(char *bufferRequest, char *bufferResponse) 
{
    /* No check on parameters. */
    GeneralSendBuffer *bufferGeneralSend = (GeneralSendBuffer *)bufferRequest; /* Send and request. */
    GeneralReceiveBuffer *bufferGeneralReceive = (GeneralReceiveBuffer *)bufferResponse; /* Receive and response. */
    bufferGeneralReceive->message = MESSAGE_RESPONSE; /* Fill response message. */
    Debug::debugItem("Debug-filesystem.cpp: parseMessage once");
    switch(bufferGeneralSend->message) {
        case MESSAGE_ADDMETATODIRECTORY: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_ADDMETATODIRECTORY");
            AddMetaToDirectorySendBuffer *bufferSend = 
                (AddMetaToDirectorySendBuffer *)bufferGeneralSend;
            UpdataDirectoryMetaReceiveBuffer *bufferReceive = (UpdataDirectoryMetaReceiveBuffer *)bufferResponse;
            bufferReceive->result = addMetaToDirectory(
                bufferSend->path, bufferSend->name, 
                bufferSend->isDirectory, 
                &(bufferReceive->TxID),
                &(bufferReceive->srcBuffer),
                &(bufferReceive->desBuffer),
                &(bufferReceive->size),
                &(bufferReceive->key),
                &(bufferReceive->offset));
            break;
        }
        case MESSAGE_REMOVEMETAFROMDIRECTORY: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_REMOVEMETAFROMDIRECTORY");
            RemoveMetaFromDirectorySendBuffer *bufferSend = 
                (RemoveMetaFromDirectorySendBuffer *)bufferGeneralSend;
            UpdataDirectoryMetaReceiveBuffer *bufferReceive = (UpdataDirectoryMetaReceiveBuffer *)bufferResponse;
            bufferGeneralReceive->result = removeMetaFromDirectory(
                bufferSend->path, bufferSend->name,
                &(bufferReceive->TxID),
                &(bufferReceive->srcBuffer),
                &(bufferReceive->desBuffer),
                &(bufferReceive->size),
                &(bufferReceive->key),
                &(bufferReceive->offset));
            break;
        }
        case MESSAGE_DOCOMMIT:
        {
	    Debug::debugItem("parseMessage: MESSAGE_DOCOMMIT");
            DoRemoteCommitSendBuffer *bufferSend = (DoRemoteCommitSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = updateDirectoryMeta(
                bufferSend->path, bufferSend->TxID, bufferSend->srcBuffer, 
                bufferSend->desBuffer, bufferSend->size, bufferSend->key, bufferSend->offset);
            break;
        }
        case MESSAGE_MKNOD: 
        {
 	    Debug::debugItem("parseMessage: MESSAGE_MKNOD");
            bufferGeneralReceive->result = mknod(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_GETATTR: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_GETATTR");
            GetAttributeReceiveBuffer *bufferReceive = 
                (GetAttributeReceiveBuffer *)bufferGeneralReceive;
	    FileMeta *attr = (FileMeta *) malloc(sizeof(FileMeta));
            bufferReceive->result = getattr(bufferGeneralSend->path, attr, bufferReceive->BlockList);
	    bufferReceive->attribute.size = attr->size;
	    bufferReceive->attribute.count = attr->count;
	    Debug::debugItem("BlockID %d, node %d, tier %d", bufferReceive->BlockList[0].BlockID, bufferReceive->BlockList[0].nodeID, bufferReceive->BlockList[0].tier);
            break;
        }
        case MESSAGE_ACCESS: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_ACCESS");
	    bool isDirectory = false;
            bufferGeneralReceive->result = access(bufferGeneralSend->path, &isDirectory);
	    if (!isDirectory) {
		Debug::debugItem("Not a Directory");
		bufferGeneralReceive->message = MESSAGE_NOTDIR;
	    }
            break;
        }
        case MESSAGE_MKDIR: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_MKDIR");
            bufferGeneralReceive->result = mkdir(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_READDIR: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_READDIR");
            ReadDirectoryReceiveBuffer *bufferReceive = 
                (ReadDirectoryReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = readdir(bufferGeneralSend->path, &(bufferReceive->list));
            break;
        }
        case MESSAGE_READDIRECTORYMETA:
        {
		Debug::debugItem("parseMessage: MESSAGE_READDIRECTORYMETA");
        	ReadDirectoryMetaReceiveBuffer *bufferReceive = 
        		(ReadDirectoryMetaReceiveBuffer *)bufferGeneralReceive;
        	bufferReceive->result = readDirectoryMeta(bufferGeneralSend->path, 
        		&(bufferReceive->meta), 
        		&(bufferReceive->hashAddress),
        		&(bufferReceive->metaAddress),
        		&(bufferReceive->parentNodeID));
        	break;
        }
        case MESSAGE_EXTENTREAD:
        {
	    Debug::debugItem("parseMessage: MESSAGE_EXTENTREAD");
            ExtentReadSendBuffer *bufferSend = 
                (ExtentReadSendBuffer *)bufferGeneralSend;
            ExtentReadReceiveBuffer *bufferReceive = 
                (ExtentReadReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentRead(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi),
                 &(bufferReceive->offset), &(bufferReceive->key));
            unlockReadHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_EXTENTWRITE:
        {
	    Debug::debugItem("parseMessage: MESSAGE_EXTENTWRITE");
            ExtentWriteSendBuffer *bufferSend = 
                (ExtentWriteSendBuffer *)bufferGeneralSend;
            ExtentWriteReceiveBuffer *bufferReceive = 
                (ExtentWriteReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentWrite(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi), 
                &(bufferReceive->offset), &(bufferReceive->key));
            unlockWriteHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_UPDATEMETA:
        {
	    Debug::debugItem("parseMessage: MESSAGE_UPDATEMETA");
            UpdateMetaSendBuffer *bufferSend = (UpdateMetaSendBuffer *)bufferGeneralSend;
	    //uint32_t tier = 0; /*To be implemented: Intelligent data palcement*/
	    bufferGeneralReceive->result = true;
            //bufferGeneralReceive->result = updateMeta(bufferSend->path, &(bufferSend->metaFile), bufferSend->key);
            break;
        }
        case MESSAGE_EXTENTREADEND:
        {
	    Debug::debugItem("parseMessage: MESSAGE_EXTENTREADEND");
            // ExtentReadEndSendBuffer *bufferSend = 
            //     (ExtentReadEndSendBuffer *)bufferGeneralSend;
            // bufferGeneralReceive->result = extentReadEnd(bufferSend->key, bufferSend->path);
            break;
        }
        case MESSAGE_TRUNCATE: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_TRUNCATE");
            TruncateSendBuffer *bufferSend = 
                (TruncateSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = truncate(bufferSend->path, bufferSend->size);
            break;
        }
        case MESSAGE_RMDIR: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_RMDIR");
            bufferGeneralReceive->result = rmdir(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_REMOVE:
        {
	    Debug::debugItem("parseMessage: MESSAGE_REMOVE");
            GetAttributeReceiveBuffer *bufferReceive = (GetAttributeReceiveBuffer *)bufferGeneralReceive;
            //GeneralReceiveBuffer *bufferReceive = (GeneralReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = remove(bufferGeneralSend->path, &(bufferReceive->attribute));
            break;
        }
        case MESSAGE_FREEBLOCK:
        {
	    Debug::debugItem("parseMessage: MESSAGE_FREEBLOCK");
            BlockFreeSendBuffer *bufferSend = 
                (BlockFreeSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = blockFree(bufferSend->startBlock, bufferSend->countBlock);
            break;
        }
        case MESSAGE_MKNODWITHMETA: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_MKNODWITHMETA");
            MakeNodeWithMetaSendBuffer *bufferSend = 
                (MakeNodeWithMetaSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = mknodWithMeta(bufferSend->path, &(bufferSend->metaFile));
            break;
        }
        case MESSAGE_RENAME: 
        {
	    Debug::debugItem("parseMessage: MESSAGE_RENAME");
            RenameSendBuffer *bufferSend = 
                (RenameSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = rename(bufferSend->pathOld, bufferSend->pathNew);
            break;
        }
        case MESSAGE_RAWWRITE:
        {
	    Debug::debugItem("parseMessage: MESSAGE_RAWWRITE");
            ExtentWriteSendBuffer *bufferSend = 
                (ExtentWriteSendBuffer *)bufferGeneralSend;
            ExtentWriteReceiveBuffer *bufferReceive = 
                (ExtentWriteReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentWrite(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi), 
                &(bufferReceive->offset), &(bufferReceive->key));
            unlockWriteHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_RAWREAD:
        {
	    Debug::debugItem("parseMessage: MESSAGE_RAWREAD");
            ExtentReadSendBuffer *bufferSend = 
                (ExtentReadSendBuffer *)bufferGeneralSend;
            ExtentReadReceiveBuffer *bufferReceive = 
                (ExtentReadReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentRead(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi),
                 &(bufferReceive->offset), &(bufferReceive->key));
            unlockReadHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
	case MESSAGE_CREATEBLOCK:
	{
	    Debug::debugItem("parseMessage: MESSAGE_CREATEBLOCK");
	    BlockRequestSendBuffer *bufferSend = (BlockRequestSendBuffer *) bufferGeneralSend;
	    BlockRequestReceiveBuffer *bufferReceive = (BlockRequestReceiveBuffer *) bufferGeneralReceive;
	    BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
            newBlock->BlockID = bufferSend->BlockID;
	    newBlock->nodeID = (uint16_t)hashLocalNode;
	    newBlock->tier = bufferSend->Storagetier;
	    newBlock->StorageAddress = bufferSend->StorageAddress;
	    bufferReceive->result = createNewBlock(newBlock);
	    break;
	}
	case MESSAGE_READBLOCK:
	{
	    Debug::debugItem("parseMessage: MESSAGE_READBLOCK");
	    BlockRequestSendBuffer *bufferSend = (BlockRequestSendBuffer *) bufferGeneralSend;
            BlockRequestReceiveBuffer *bufferReceive = (BlockRequestReceiveBuffer *) bufferGeneralReceive;
            BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
	    /*To be implemented, Juge if a block is in RDMA region*/
	    newBlock->BlockID = bufferSend->BlockID;
	    newBlock->nodeID = (uint16_t)hashLocalNode;
	    newBlock->tier = bufferSend->Storagetier;
	    newBlock->StorageAddress = bufferSend->StorageAddress;
	    bufferReceive->result = fillRDMARegionV2(bufferSend->uniqueHashValue, newBlock->BlockID, newBlock->tier, newBlock->StorageAddress, bufferSend->writeOperation);
	    break;
	}
	case MESSAGE_REMOVEBLOCK:
	{
	    Debug::debugItem("parseMessage: MESSAGE_REMOVEBLOCK");
	    BlockRequestSendBuffer *bufferSend = (BlockRequestSendBuffer *) bufferGeneralSend;
            BlockRequestReceiveBuffer *bufferReceive = (BlockRequestReceiveBuffer *) bufferGeneralReceive;
            BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
	    newBlock->BlockID = bufferSend->BlockID;
            newBlock->nodeID = (uint16_t)hashLocalNode;
            newBlock->tier = bufferSend->Storagetier;
            newBlock->StorageAddress = bufferSend->StorageAddress;
	    bufferReceive->result = removeBlock(bufferSend->uniqueHashValue, newBlock->tier, newBlock->StorageAddress);
	    break;
	}
        default:
            break;
    }
}


/* Internal add meta to directory function. Might cause overhead. No check on parameters for internal function.
   @param   path            Path of directory.
   @param   name            Name of meta.
   @param   isDirectory     Judge if it is directory.
   @return                  If succeed return true, otherwise return false. */
bool FileSystem::addMetaToDirectory(const char *path, const char *name, bool isDirectory, 
    uint64_t *TxID, uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size, uint64_t *key, uint64_t *offset)
{
    Debug::debugTitle("FileSystem::addMetaToDirectory");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
    uint64_t LocalTxID;
    if (checkLocal(hashNode) == true) { /* If local node. */
        // return true;
        bool result;
        *key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
        *offset = (uint64_t)hashAddress;
        Debug::debugItem("key = %lx, offset = %lx", *key, *offset);
        {
            Debug::debugItem("Stage 2. Check directory.");
            uint64_t indexDirectoryMeta; /* Meta index of directory. */
            bool isDirectoryTemporary; /* Different from parameter isDirectory. */
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectoryTemporary) == false) { /* If directory does not exist. */
                result = false;         /* Fail due to directory path does not exist. In future detail error information should be returned and independent access() and create() functions should be offered. */
            } else {
                if (isDirectoryTemporary == false) { /* If not a directory. */
                    result = false;     /* Fail due to path is not directory. */
                } else {
                    DirectoryMeta metaDirectory;
                    if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) { /* Get directory meta. */
                        result = false; /* Fail due to get directory meta error. */
                    } else {
                        LocalTxID = TxLocalBegin();
                        metaDirectory.count++; /* Add count of names under directory. */
                        //printf("metaDirectory.count: %d, name len: %d\n", metaDirectory.count, (int)strlen(name));
                        strcpy(metaDirectory.tuple[metaDirectory.count - 1].names, name); /* Add name. */
                        metaDirectory.tuple[metaDirectory.count - 1].isDirectories = isDirectory; /* Add directory state. */
                        /* Write to log first. */
                        TxWriteData(LocalTxID, (uint64_t)&metaDirectory, (uint64_t)sizeof(DirectoryMeta));
                        *srcBuffer = getTxWriteDataAddress(LocalTxID);
                        *size = (uint64_t)sizeof(DirectoryMeta);
                        // printf("%s ", path);
                        *TxID = LocalTxID;
                        if (storage->tableDirectoryMeta->put(indexDirectoryMeta, &metaDirectory, desBuffer) == false) { /* Update directory meta. */
                            result = false; /* Fail due to put directory meta error. */
                        } else {
                            printf("addmeta, desBuffer = %lx, srcBuffer = %lx, size = %d\n", *desBuffer, *srcBuffer, *size);
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
        }
        if (result == false) {
            TxLocalCommit(LocalTxID, false);
        } else {
            TxLocalCommit(LocalTxID, true);
        }
        // unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
        Debug::debugItem("Stage end.");
        return result;                  /* Return specific result. */
    } else {                            /* If remote node. */
        AddMetaToDirectorySendBuffer bufferAddMetaToDirectorySend; /* Send buffer. */
	    bufferAddMetaToDirectorySend.message = MESSAGE_ADDMETATODIRECTORY; /* Assign message type. */
	    strcpy(bufferAddMetaToDirectorySend.path, path);  /* Assign path. */
	    strcpy(bufferAddMetaToDirectorySend.name, name);  /* Assign name. */
        bufferAddMetaToDirectorySend.isDirectory = isDirectory;
        UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive;
        RdmaCall((uint16_t)hashNode, 
                 (char *)&bufferAddMetaToDirectorySend, 
                 (uint64_t)sizeof(AddMetaToDirectorySendBuffer),
                 (char *)&bufferGeneralReceive,
                 (uint64_t)sizeof(UpdataDirectoryMetaReceiveBuffer));
        *srcBuffer = bufferGeneralReceive.srcBuffer;
        *desBuffer = bufferGeneralReceive.desBuffer;
        *TxID = bufferGeneralReceive.TxID;
        *size = bufferGeneralReceive.size;
        *key = bufferGeneralReceive.key;
        *offset = bufferGeneralReceive.offset;
        return bufferGeneralReceive.result;
    }
}

/* Internal remove meta from directory function. Might cause overhead. No check on parameters for internal function.
   @param   path            Path of directory.
   @param   name            Name of meta.
   @param   isDirectory     Judge if it is directory.
   @return                  If succeed return true, otherwise return false. */
bool FileSystem::removeMetaFromDirectory(const char *path, const char *name, 
    uint64_t *TxID, uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size,  uint64_t *key, uint64_t *offset)
{
    Debug::debugTitle("FileSystem::removeMetaFromDirectory");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
    uint64_t LocalTxID;
    if (checkLocal(hashNode) == true) { /* If local node. */
        bool result;
        *key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
        *offset = (uint64_t)hashAddress;
        {
            Debug::debugItem("Stage 2. Check directory.");
            uint64_t indexDirectoryMeta; /* Meta index of directory. */
            bool isDirectory;
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If directory does not exist. */
            	Debug::notifyError("Directory does not exist.");
                result = false;         /* Fail due to directory path does not exist. In future detail error information should be returned and independent access() and create() functions should be offered. */
            } else {
                if (isDirectory == false) { /* If not a directory. */
                    result = false;     /* Fail due to path is not directory. */
                } else {
                    DirectoryMeta metaDirectory;
                    if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) { /* Get directory meta. */
                    	Debug::notifyError("Get directory meta failed.");
                        result = false; /* Fail due to get directory meta error. */
                    } else {
                        LocalTxID = TxLocalBegin();
                        DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
                        bool found = false;
                        uint64_t indexModifiedNames = 0;
                        for (uint64_t i = 0; i < metaDirectory.count; i++) {
                            if (strcmp(metaDirectory.tuple[i].names, name) == 0) { /* If found selected name. */
                                found = true; /* Mark and continue. */
                            } else {
                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, metaDirectory.tuple[i].names); /* Copy original name to modified meta. */
                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = metaDirectory.tuple[i].isDirectories; /* Add directory state. */
                                indexModifiedNames++;
                            }
                        }
                        metaModifiedDirectory.count = indexModifiedNames; /* No need to +1. Current point to index after last one. Can be adapted to multiple removes. However it should not occur. */
                        if (found == false) {
                        	Debug::notifyError("Fail due to no selected name.");
                            TxLocalCommit(LocalTxID, false);
                            result = false; /* Fail due to no selected name. */
                        } else {
                            /* Write to log first. */
                            TxWriteData(LocalTxID, (uint64_t)&metaModifiedDirectory, (uint64_t)sizeof(DirectoryMeta));
                            *srcBuffer = getTxWriteDataAddress(LocalTxID);
                            *size = (uint64_t)sizeof(DirectoryMeta);
                            *TxID = LocalTxID;
                            if (storage->tableDirectoryMeta->put(indexDirectoryMeta, &metaModifiedDirectory, desBuffer) == false) { /* Update directory meta. */
                                result = false; /* Fail due to put directory meta error. */
                            } else {
                                Debug::debugItem("Item. ");
                                result = true; /* Succeed. */
                            }
                        }
                    }
                }
            }
        }
        if (result == false) {
            TxLocalCommit(LocalTxID, false);
        } else {
            TxLocalCommit(LocalTxID, true);
        }
        //unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
        Debug::debugItem("Stage end.");
        return result;                  /* Return specific result. */
    } else {                            /* If remote node. */
        RemoveMetaFromDirectorySendBuffer bufferRemoveMetaFromDirectorySend; /* Send buffer. */
        bufferRemoveMetaFromDirectorySend.message = MESSAGE_REMOVEMETAFROMDIRECTORY; /* Assign message type. */
        strcpy(bufferRemoveMetaFromDirectorySend.path, path);  /* Assign path. */
        strcpy(bufferRemoveMetaFromDirectorySend.name, name);  /* Assign name. */
        UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
        RdmaCall((uint16_t)hashNode,
                 (char *)&bufferRemoveMetaFromDirectorySend,
                 (uint64_t)sizeof(RemoveMetaFromDirectorySendBuffer),
                 (char *)&bufferGeneralReceive,
                 (uint64_t)sizeof(UpdataDirectoryMetaReceiveBuffer));
        *srcBuffer = bufferGeneralReceive.srcBuffer;
        *desBuffer = bufferGeneralReceive.desBuffer;
        *TxID = bufferGeneralReceive.TxID;
        *size = bufferGeneralReceive.size;
        *key = bufferGeneralReceive.key;
        *offset = bufferGeneralReceive.offset;
        return bufferGeneralReceive.result;
    }
}

bool FileSystem::updateDirectoryMeta(const char *path, uint64_t TxID, uint64_t srcBuffer, 
        uint64_t desBuffer, uint64_t size, uint64_t key, uint64_t offset) {
    Debug::debugTitle("FileSystem::updateDirectoryMeta");
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        Debug::debugItem("path = %s, TxID = %d, srcBuffer = %lx, desBuffer = %lx, size = %ld", path, TxID, srcBuffer, desBuffer, size);
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        // AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) {
            Debug::debugItem("Stage 2. Check Local.");
            bool result = true;
            // uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            uint64_t indexDirectoryMeta;
            bool isDirectory;
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path exists. */
                Debug::notifyError("The path does not exists.");
                result = false; /* Fail due to existence of path. */
            } else {
                result = true;
                // printf("%s ", path);
                Debug::debugItem("update, desbuf = %lx, srcbuf = %lx, size = %d",desBuffer, srcBuffer, size);
                memcpy((void *)desBuffer, (void *)srcBuffer, size);
                Debug::debugItem("copied");
            }
            Debug::debugItem("key = %lx, offset = %lx", key, offset);
            unlockWriteHashItem(key, hashNode, (AddressHash)offset);  /* Unlock hash item. */
            TxLocalCommit(TxID, true);
            if (result == false) {
                Debug::notifyError("DOCOMMIT With Error.");
            }
            return result;
        } else {
            DoRemoteCommitSendBuffer bufferSend;
            strcpy(bufferSend.path, path);
            bufferSend.message = MESSAGE_DOCOMMIT;
            bufferSend.TxID = TxID;
            bufferSend.srcBuffer = srcBuffer;
            bufferSend.desBuffer = desBuffer;
            bufferSend.size = size;
            bufferSend.key = key;
            bufferSend.offset = offset;
            GeneralReceiveBuffer bufferReceive;
            RdmaCall((uint16_t)hashNode,
                    (char *)&bufferSend,
                    (uint64_t)sizeof(DoRemoteCommitSendBuffer),
                    (char *)&bufferReceive,
                    (uint64_t)sizeof(GeneralReceiveBuffer));
            if (bufferReceive.result == false) {
                Debug::notifyError("Remote Call on DOCOMMIT With Error.");
            }
            return bufferReceive.result;
        }
    }
}

/* Make node (file) with file meta.
   @param   path        Path of file.
   @param   metaFile    File meta. 
   @return              If succeed return true, otherwise return false. */
bool FileSystem::mknodWithMeta(const char *path, FileMeta *metaFile)
{
    Debug::debugTitle("FileSystem::mknodWithMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (metaFile == NULL)) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            Debug::debugItem("Stage 2. Check parent.");
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 3. Create file meta from old.");
                    uint64_t indexFileMeta;
                    metaFile->timeLastModified = time(NULL); /* Set last modified time. */
                    if (storage->tableFileMeta->create(&indexFileMeta, metaFile) == false) {
                        result = false; /* Fail due to create error. */
                    } else {
                        if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                            result = false; /* Fail due to hash table put. No roll back. */
                        } else {
                            result = true;
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
} 

/* Make node. That is to create an empty file. 
   @param   path    Path of file.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::mknod(const char *path) 
{
#ifdef TRANSACTION_2PC
    return mknod2pc(path);
#endif
#ifdef TRANSACTION_CD
    return mknodcd(path);
#endif
}

bool FileSystem::mknodcd(const char *path) 
{
    Debug::debugTitle("FileSystem::mknod-cd");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                // DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Update parent directory metadata.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                DirectoryMeta parentMeta;
                uint64_t parentHashAddress, parentMetaAddress;
                uint16_t parentNodeID;
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                    Debug::notifyError("readDirectoryMeta failed.");
                    // TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory = false;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        Debug::notifyError("addMetaToDirectory failed.");
                        // TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                    	
                    	/* Update directory meta first. */
                    	parentMeta.count++; /* Add count of names under directory. */
                        strcpy(parentMeta.tuple[parentMeta.count - 1].names, name); /* Add name. */
                        parentMeta.tuple[parentMeta.count - 1].isDirectories = isDirectory; /* Add directory state. */
                        
                        Debug::debugItem("Stage 3. Create file meta. name is %s, isDirectory = %d", name, isDirectory);
                        uint64_t indexFileMeta;
                        FileMeta metaFile;
                        metaFile.timeLastModified = time(NULL); /* Set last modified time. */
                        metaFile.count = 0; /* Initialize count of extents as 0. */
                        metaFile.size = 0;
			metaFile.isNewFile = true;
			metaFile.tier = 0;
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        // TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
			if (parentNodeID == server->getRdmaSocketInstance()->getNodeID()) {
			    UniqueHash dirhashUnique;
			    HashTable::getUniqueHash(parent, strlen(parent), &dirhashUnique);
			    uint64_t indexDirectoryMeta;
			    bool isDirectoryTemporary;
			    storage->hashtable->get(&dirhashUnique, &indexDirectoryMeta, &isDirectoryTemporary);
			    Debug::debugItem("updateMetaV2, put metadata to tableDirectoryMeta, indexDirectoryMeta = %ld", (long)indexDirectoryMeta);
			    Debug::debugItem("countSavedItems = %ld", storage->tableDirectoryMeta->countSavedItems());
			    storage->tableDirectoryMeta->put(indexDirectoryMeta, &parentMeta);
			} else {
                            updateRemoteMeta(parentNodeID, &parentMeta, parentMetaAddress, parentHashAddress);
			}
                        /* Only allocate momery, write to log first. */
                        if (storage->tableFileMeta->create(&indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
            	free(name);
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}
bool FileSystem::mknod2pc(const char *path) 
{
    printf("Debug-fileystem.cpp: mknod-2pc\n");
    Debug::debugTitle("FileSystem::mknod");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Update parent directory metadata.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (addMetaToDirectory(parent, name, false, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    Debug::notifyError("addMetaToDirectory failed.");
                    TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        Debug::notifyError("addMetaToDirectory failed.");
                        TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                        Debug::debugItem("Stage 3. Create file meta.");
                        uint64_t indexFileMeta;
                        FileMeta metaFile;
                        metaFile.timeLastModified = time(NULL); /* Set last modified time. */
                        metaFile.count = 0; /* Initialize count of extents as 0. */
                        metaFile.size = 0;
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&metaFile, (uint64_t)sizeof(FileMeta));
                        /* Receive remote prepare with (OK) */
                        TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        Debug::debugItem("mknod, key = %lx, offset = %lx", remotekey, offset);
                        updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                        /* Only allocate momery, write to log first. */
                        if (storage->tableFileMeta->create(&indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
                free(name);
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}
/* Get attributes. 
   @param   path        Path of file or folder.
   @param   attribute   Attribute buffer of file to get.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::getattr(const char *path, FileMeta *attribute, BlockInfo BlockList[MAX_MESSAGE_BLOCK_COUNT])
{
    Debug::debugTitle("FileSystem::getattr");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (attribute == NULL)) /* Judge if path and attribute buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            // return true;
            bool result;
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory = false;
                Debug::debugItem("Stage 1.1.");
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    Debug::debugItem("Stage 1.2.");
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                        if (storage->tableFileMeta->get(indexMeta, attribute) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            Debug::debugItem("FileSystem::getattr, meta.size = %ld, mete.count = %d", (long) attribute->size, attribute->count);
			    for (int i = 0; i < attribute->count; i ++) {
				BlockList[i] = attribute->BlockList[i];
				if (i == (MAX_MESSAGE_BLOCK_COUNT-1))
				    break;
			    }
                            result = true; /* Succeed. */
                        }
                    } else {
                    	attribute->count = MAX_FILE_EXTENT_COUNT; /* Inter meaning, representing directoiries */
                        result = true;
                    }
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Access. That is to judge the existence of file or folder.
   @param   path    Path of file or folder.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::access(const char *path, bool *isDirectory2)
{
    Debug::debugTitle("FileSystem::access");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL)                   /* Judge if path is valid. */
        return false;                   /* Null path error. */
    else {
        Debug::debugItem("Stage 2. Check access.");
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        Debug::debugItem("Stage 3.");
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            Debug::debugItem("Stage 4.");
	    bool isDirectory;
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
		    Debug::debugItem("Stage 5. retrun false, path does not exist.");
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    result = true;      /* Succeed. Do not need to check parent. */
		    if (isDirectory) {
			Debug::debugItem("Stage 5. retrun true, Dir exists.");
			*isDirectory2 = true;
		    } else {
 		        Debug::debugItem("Stage 5. retrun true, file exists.");
			*isDirectory2 = false;
		    }
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Make directory. 
   @param   path    Path of folder. 
   @return          If operation succeeds then return true, otherwise return false. */
// bool FileSystem::mkdir(const char *path)
// {
//     Debug::debugTitle("FileSystem::mkdir");
//     Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
//     char *parent = (char *)malloc(strlen(path) + 1);
//     getParentDirectory(path, parent);
//     nrfsfileattr attribute;
//     Debug::debugItem("Stage 2. Check parent.");
//     getattr(parent, &attribute);
//     char *name = (char *)malloc(strlen(path) + 1); /* Allocate buffer of parent path. Omit failure. */
//     getNameFromPath(path, name);
//     Debug::debugItem("Stage 3. Add name.");
//     addMetaToDirectory(parent, name, true);
//     free(parent);
//     free(name);
//     return true;
// }

bool FileSystem::mkdir(const char *path) 
{
#ifdef TRANSACTION_2PC
    return mkdir2pc(path);
#endif
#ifdef TRANSACTION_CD
    return mkdircd(path);
#endif
}

bool FileSystem::mkdircd(const char *path)
{
    Debug::debugTitle("FileSystem::mkdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                // DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Check parent.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                DirectoryMeta parentMeta;
                uint64_t parentHashAddress, parentMetaAddress;
                uint16_t parentNodeID;
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                    // TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        // TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {

                    	/* Update directory meta first. */
                    	parentMeta.count++; /* Add count of names under directory. */
                        strcpy(parentMeta.tuple[parentMeta.count - 1].names, name); /* Add name. */
                        parentMeta.tuple[parentMeta.count - 1].isDirectories = true; /* Add directory state. */
                        
                        Debug::debugItem("Stage 3. Write directory meta.");
                        uint64_t indexDirectoryMeta;
                        DirectoryMeta metaDirectory;
                        metaDirectory.count = 0; /* Initialize count of names as 0. */
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        // TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        updateRemoteMeta(parentNodeID, &parentMeta, parentMetaAddress, parentHashAddress);
                        
                        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            Debug::debugItem("indexDirectoryMeta = %d", indexDirectoryMeta);
                            if (storage->hashtable->put(&hashUnique, indexDirectoryMeta, true) == false) { /* true for directory. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
            	free(name);
            }

            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }

            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::mkdir2pc(const char *path)
{
    Debug::debugTitle("FileSystem::mkdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Check parent.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (addMetaToDirectory(parent, name, true, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                        Debug::debugItem("Stage 3. Write directory meta.");
                        uint64_t indexDirectoryMeta;
                        DirectoryMeta metaDirectory;
                        metaDirectory.count = 0; /* Initialize count of names as 0. */
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&metaDirectory, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        Debug::debugItem("mknod, key = %lx, offset = %lx", remotekey, offset);
                        updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);

                        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            Debug::debugItem("indexDirectoryMeta = %d", indexDirectoryMeta);
                            if (storage->hashtable->put(&hashUnique, indexDirectoryMeta, true) == false) { /* true for directory. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
                free(name);
            }

            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }

            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Read filenames in directory. 
   @param   path    Path of folder.
   @param   list    List buffer of names in directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::readdir(const char *path, nrfsfilelist *list)
{
    Debug::debugTitle("FileSystem::readdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (list == NULL)) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                        result = false; /* Fail due to not directory. */
                    } else {
                        DirectoryMeta metaDirectory; /* Copy operation of meta might cause overhead. */
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                            list->count = metaDirectory.count; /* Assign count of names in directory. */
                            for (uint64_t i = 0; i < metaDirectory.count; i++) {
                                strcpy(list->tuple[i].names, metaDirectory.tuple[i].names);
                                if (metaDirectory.tuple[i].isDirectories == true) {
                                    list->tuple[i].isDirectories = true; /* Mode 1 for directory. */
                                } else { 
                                    list->tuple[i].isDirectories = false; /* Mode 0 for file. */
                                }
                            }
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Read filenames in directory. 
   @param   path    Path of folder.
   @param   list    List buffer of names in directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::recursivereaddir(const char *path, int depth)
{
    if (path == NULL) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    if (isDirectory == false) { /* If file meta. */
                        result = false; /* Fail due to not directory. */
                    } else {
                        DirectoryMeta metaDirectory; /* Copy operation of meta might cause overhead. */
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                            for (uint64_t i = 0; i < metaDirectory.count; i++) {
				for (int nn = 0; nn < depth; nn++)
					printf("\t");
                                if (metaDirectory.tuple[i].isDirectories == true) {
					printf("%d DIR %s\n", (int)i, metaDirectory.tuple[i].names);
					char *childPath = (char *)malloc(sizeof(char) * 
						(strlen(path) + strlen(metaDirectory.tuple[i].names) + 2));
					strcpy(childPath, path);
					if (strcmp(childPath, "/") != 0)
						strcat(childPath, "/");
					strcat(childPath, metaDirectory.tuple[i].names);
					recursivereaddir(childPath, depth + 1);
					free(childPath);
                                } else {
					printf("%d FILE %s\n", (int)i, metaDirectory.tuple[i].names);
				}
                            }
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}


/* Read directory meta. 
   @param   path    Path of folder.
   @param   meta    directory meta pointer.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::readDirectoryMeta(const char *path, DirectoryMeta *meta, uint64_t *hashAddress, uint64_t *metaAddress, uint16_t *parentNodeID) {
    Debug::debugTitle("FileSystem::readDirectoryMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
    	bool result;
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        *hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        *parentNodeID = (uint16_t)hashNode;
        if (checkLocal(hashNode) == true) { /* If local node. */
	    Debug::debugItem("readDirectoryMeta : local node");
            uint64_t key = lockReadHashItem(hashNode, *hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                	Debug::notifyError("path does not exist");
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                    	Debug::notifyError("Not a directory");
                        result = false; /* Fail due to not directory. */
                    } else {
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, meta, metaAddress) == false) {
                            Debug::notifyError("Fail due to get directory meta error, indexDirectoryMeta = %ld", (long)indexDirectoryMeta);
			    Debug::notifyError("countSavedItems = %ld", storage->tableDirectoryMeta->countSavedItems());
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                        	Debug::debugItem("metaAddress = %lx, getDmfsBaseAddress = %lx", *metaAddress, server->getMemoryManagerInstance()->getDmfsBaseAddress());
                        	*metaAddress = *metaAddress - server->getMemoryManagerInstance()->getDmfsBaseAddress();
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockReadHashItem(key, hashNode, *hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
		Debug::debugItem("readDirectoryMeta : remote node");
        	GeneralSendBuffer bufferSend;
        	bufferSend.message = MESSAGE_READDIRECTORYMETA;
        	strcpy(bufferSend.path, path);
        	ReadDirectoryMetaReceiveBuffer bufferReceive;
        	RdmaCall((uint16_t)hashNode,
                    (char *)&bufferSend,
                    (uint64_t)sizeof(GeneralSendBuffer),
                    (char *)&bufferReceive,
                    (uint64_t)sizeof(ReadDirectoryMetaReceiveBuffer));
            if (bufferReceive.result == false) {
            	Debug::notifyError("Remote Call readDirectoryMeta failed");
                result = false;
            } else {
            	memcpy((void *)meta, (void *)&(bufferReceive.meta), sizeof(DirectoryMeta));
            	*hashAddress = bufferReceive.hashAddress;
            	*metaAddress = bufferReceive.metaAddress;
            	*parentNodeID = bufferReceive.parentNodeID;
            	return bufferReceive.result;
            }
            return false;
        }
    }
}


 /*There are 2 schemas of blocks.
 * 
 *      Schema 1, only one block: ("[" means start byte and "]" means end byte.)
 *
 *      Bound of Block     + start_block = end_block 
 *           Index         |
 *      -------------------+----------------------------------------+--------------------
 *                         |                                        |
 *        Previous Blocks  |        Start Block  (End Block)        |  Following Blocks
 *                         |                                        |
 *      -------------------+-------[-----------------------]--------+--------------------
 *                                 |                       |
 *      Relative Address           + offset                + offset + size - 1
 *          in File                \                      /
 *                                  -------  size  -------
 *
 *      Schema 2, several blocks:
 *
 *      Bound of Block     + start_block                                         + end_block
 *           Index         |                                                     |
 *      -------------------+---------------+-------------------------------------+-------------+--------------------
 *                         |               |          Middle Blocks              |             |
 *        Previous Blocks  |  Start Block  +-----+-----+---------+-----+---------+  End Block  |  Following Blocks
 *                         |               |  0  |  1  |   ...   |  i  |   ...   |             |
 *      -------------------+----[----------[-----+-----+---------[-----+---------+---------]---+--------------------
 *                              |          |                     |                         |
 *     start_block * BLOCK_SIZE +          + (start_block + 1)   + (start_block + 1 + i)   + start_block * BLOCK_SIZE 
 *      + offset % BLOCK_SIZE   \             * BLOCK_SIZE          * BLOCK_SIZE          /   + offset % BLOCK_SIZE + size - 1
 *                               ------------------------ size  --------------------------
 *
 *                              +----------------------------------------------------------+
 *                              |                                                          |
 *                              |                       Buffer                             |
 *                              |                                                          |
 *                              [----------[-----+-----+---------[-----+---------[---------]
 *                              |          |     |     |   ...   |     |   ...   |         |
 *                              + 0        |     +     +         |     +         |         + size - 1
 *                                         |                     |               |
 *                                         |                     |             + BLOCK_SIZE - offset % BLOCK_SIZE + (end_block - start_block - 1) * BLOCK_SIZE
 *                                         |                     |
 *                                         |                     + BLOCK_SIZE - offset % BLOCK_SIZE + i * BLOCK_SIZE
 *                                         |
 *                                         + BLOCK_SIZE - offset % BLOCK_SIZE
 *
 *      start_block = offset / BLOCK_SIZE
 *      end_block = (offset + size - 1) / BLOCK_SIZE
 */


/* Fill file position information for read and write. No check on parameters.
   @param   size        Size to operate.
   @param   offset      Offset to operate. 
   @param   fpi         File position information.
   @param   metaFile    File meta. */
void FileSystem::fillFilePositionInformation(uint64_t size, uint64_t offset, file_pos_info *fpi, FileMeta *metaFile) {
    Debug::debugItem("Stage 8.");
    uint64_t offsetStart, offsetEnd;
    offsetStart = offset;  /* Relative offset of start byte to operate in file. */
    offsetEnd = size + offset - 1; /* Relative offset of end byte to operate in file. */
    uint64_t boundStartExtent, boundEndExtent, /* Bound of start extent and end extent. */
             offsetInStartExtent, offsetInEndExtent, /* Offset of start byte in start extent and end byte in end extent. */
             sizeInStartExtent, sizeInEndExtent; /* Size to operate in start extent and end extent. */
    uint64_t offsetStartOfCurrentExtent = 0; /* Relative offset of start byte in current extent. */
    Debug::debugItem("Stage 9.");
    /*Special operation for scenario that each extent contains one block only*/
    boundStartExtent = offset / BLOCK_SIZE;
    boundEndExtent = (offset + size - 1) / BLOCK_SIZE;
    offsetInStartExtent = offset % BLOCK_SIZE;
    sizeInStartExtent = (offset % BLOCK_SIZE + size) > BLOCK_SIZE ? (BLOCK_SIZE - offset % BLOCK_SIZE): size;
    if (boundStartExtent == boundEndExtent) {
        sizeInEndExtent = size;
    } else {
        sizeInEndExtent = size - (boundEndExtent - boundStartExtent - 1) * BLOCK_SIZE - sizeInStartExtent;
    }

    Debug::debugItem("Stage 11. boundStartExtent = %lu, boundEndExtent = %lu", boundStartExtent, boundEndExtent);
    if (boundStartExtent == boundEndExtent) { /* If in one extent. */
        fpi->len = 1;                   /* Assign length. */
        fpi->tuple[0].node_id = metaFile->BlockList[boundStartExtent].nodeID; /* Assign node ID. */
        fpi->tuple[0].offset = metaFile->BlockList[boundStartExtent].indexCache * BLOCK_SIZE + offsetInStartExtent; /* Assign offset. */
        fpi->tuple[0].size = size;
    } else {                            /* Multiple extents. */
        Debug::debugItem("Stage 12.");
        fpi->len = boundEndExtent - boundStartExtent + 1; /* Assign length. */
        fpi->tuple[0].node_id = metaFile->BlockList[boundStartExtent].nodeID; /* Assign node ID of start extent. */
        fpi->tuple[0].offset = metaFile->BlockList[boundStartExtent].indexCache * BLOCK_SIZE + offsetInStartExtent; /* Assign offset. */
        fpi->tuple[0].size = sizeInStartExtent; /* Assign size. */
        for (int i = 1; i <= ((int)(fpi->len) - 2); i++) { /* Start from second extent to one before last extent. */
            fpi->tuple[i].node_id = metaFile->BlockList[boundStartExtent + i].nodeID; /* Assign node ID of start extent. */
            fpi->tuple[i].offset = metaFile->BlockList[boundStartExtent + i].indexCache * BLOCK_SIZE; /* Assign offset. */
            fpi->tuple[i].size = BLOCK_SIZE; /* Assign size. */
        }
        fpi->tuple[fpi->len - 1].node_id= metaFile->BlockList[boundEndExtent].nodeID; /* Assign node ID of start extent. */
        fpi->tuple[fpi->len - 1].offset = 0;  /* Assign offset. */
        fpi->tuple[fpi->len - 1].size = sizeInEndExtent; /* Assign size. */
        Debug::debugItem("Stage 13.");
    }
}

/*Fill RDMA Region for remote read/write request*/
bool FileSystem::fillRDMARegionV2(uint64_t uniqueHashValue, uint64_t BlockID, uint16_t tier, uint64_t StorageAddress, bool writeOperation) {
    bool ret = false;
    Debug::debugItem("Move data to RDMA region for remote read");
    uint64_t RdmaZoneBaseAddress = server->getMemoryManagerInstance()->getDataAddress();
    uint64_t indexCurrentExtraBlock;
    /*Init a new block*/
    BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));

    if (storage->BlockManager->exists(uniqueHashValue)) {
	*newBlock = storage->BlockManager->get(uniqueHashValue);
	return true;
    } else {
	newBlock->tier = tier;
        newBlock->isDirty = writeOperation;
        newBlock->present = true;
        newBlock->StorageAddress = StorageAddress;
	/*Evict an obsolete block first to make room for new block*/
	LRUInsert(uniqueHashValue, newBlock);
    }

    if (storage->tableBlock->create(&indexCurrentExtraBlock) == false) { /*Allocate a new block in RDMA region*/
        Debug::notifyError("Create block in RDMA region failed!");
        return false;
    } else {
	Debug::debugItem("Init RDMA block, id = %d, indexCurrentExtraBlock = %d", BlockID, indexCurrentExtraBlock);

	/*date BlcokManager*/
        newBlock->indexCache = indexCurrentExtraBlock;
        LRUInsert(uniqueHashValue, newBlock);

	if (tier == 0) {
	    void *src = (void *)StorageAddress;
	    void *dest  = (void *)(RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE);
	    memcpy(dest, src, BLOCK_SIZE);
            Debug::debugItem("Copy data from Memory tier, src = %ld, dest = %ld", (long)StorageAddress, (long)(RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE));
	    ret = true;
	} else {
	    Debug::debugItem("Copy data from SSD tier");
	    const char *key = ltos((long)uniqueHashValue).data();
            void *value  = (void *) (RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE);
	    storage->db.get((const char *)key, sizeof(key), (char *)value, BLOCK_SIZE);
            Debug::debugItem("Copy data from the SSD tier Done");
	    ret = true;
	}
    }
    return ret;
}

/*Copy data from Memory tier or SSD tier to the RDMA region.
   @param   BlockID        The block ID that need to be copied.
   @param   writeOperation Judge if this is a wirate operation*/

bool FileSystem::fillRDMARegion(uint64_t uniqueHashValue, uint64_t BlockID, BlockInfo *block, const char *path, bool writeOperation) {
    uint64_t MemZoneBaseAddress = server->getMemoryManagerInstance()->getExtraDataAddress();
    uint64_t RdmaZoneBaseAddress = server->getMemoryManagerInstance()->getDataAddress();
    uint64_t indexCurrentExtraBlock;
    /*Init a new block*/
    BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
    newBlock->BlockID = block->BlockID;
    newBlock->tier = block->tier;
    newBlock->isDirty = writeOperation;
    newBlock->present = true;
    newBlock->StorageAddress = block->StorageAddress;

    /*If no enough space, evict an obsolete block first*/
    //if(!BlockManager->exists(uniqueHashValue)) {
    //    LRUInsert(uniqueHashValue, newBlock);
    //}
    if (storage->tableBlock->create(&indexCurrentExtraBlock) == false) { /*Allocate a new block in RDMA region*/
        Debug::debugItem("Create block in RDMA region failed!");
	Debug::notifyError("Create block in RDMA region failed!");
        return false;
    } else {
	Debug::debugItem("Init RDMA block, id = %d, indexCurrentExtraBlock = %d", BlockID, indexCurrentExtraBlock);

	newBlock->indexCache = indexCurrentExtraBlock;

	/*Copy data*/
	if(newBlock->tier == 0) {
	    void *src = (void *)(block->StorageAddress);
            void *dest  = (void *)(RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE);
            Debug::debugItem("src = %ld, dest = %ld", (long)block->StorageAddress, (long)(RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE));
            memcpy(dest, src, BLOCK_SIZE);
            Debug::debugItem("Copy data from Memory tier, src = %ld", (long)block->StorageAddress);
	} else {
           Debug::debugItem("Copy data from SSD tier");
	   const char *key = ltos((long)uniqueHashValue).data();
           void *value  = (void *) (RdmaZoneBaseAddress + indexCurrentExtraBlock * BLOCK_SIZE);
           storage->db.get((const char *)key, sizeof(key), (char *)value, BLOCK_SIZE);
           Debug::debugItem("Copy data from the SSD tier Done");
        }
        /*update BlcokManager*/
        LRUInsert(uniqueHashValue, newBlock);
    }
    return true;
}


/* Read extent end. Only unlock path due to lock in extentRead.
   @param   Key         key obtained from read lock.
   @param   path        Path of file or folder.*/
bool FileSystem::extentReadEnd(uint64_t key, char* path)
{
    Debug::debugTitle("FileSystem::extentReadEnd");

    if (path == NULL)                   /* Judge if path and file meta buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            /* Do not lock. It has been locked in extentRead. */
            // uint64_t key = lockHashItem(path); /* Lock hash item. */
            uint64_t indexFileMeta;
            bool isDirectory;
            storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory);
            if(isDirectory)
                Debug::notifyError("Directory is read.");
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return true;                     /* Return. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Update meta. Only unlock path due to lock in extentWrite.
   @param   path        Path of file or folder.
   @param   metaFile    File meta to update.
   @oaram   key         Key to unlock.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::updateMeta(const char *path, FileMeta *metaFile, uint64_t key)
{
    Debug::debugTitle("FileSystem::updateMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (metaFile == NULL)) /* Judge if path and file meta buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            /* Do not lock. It has been locked in extentWrite. */
            // uint64_t key = lockHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Put meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false;
                    } else {
                    	metaFile->timeLastModified = time(NULL); /* Set last modified time. */
                        if (storage->tableFileMeta->put(indexFileMeta, metaFile) == false) {
                            result = false; /* Fail due to put file meta error. */
                        } else {
                            Debug::debugItem("stage 3. Update Meta Succeed.");
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Truncate file. Currently only support shrink file but cannot enlarge.
   @param   path    Path of file.
   @param   size    Size of file. 
   @return          If operation succeeds then return 0, otherwise return negative value. */
bool FileSystem::truncate(const char *path, uint64_t size) /* Truncate size of file to 0. */
{
    Debug::debugTitle("FileSystem::truncate");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false;
                    } else {
                        FileMeta metaFile;
                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            if (size >= metaFile.size) { /* So metaFile.size won't be 0. At least one block. */
                                result = false; /* Fail due to no bytes need to removed. */
                            } else {
                                uint64_t countTotalBlockTillLastExtentEnd = 0; /* Count of total blocks till end of last extent. */
                                uint64_t countNewTotalBlock; /* Count of total blocks in new file. */
                                if (size == 0) {
                                    countNewTotalBlock = 0; /* For 0 size file. */
                                } else {
                                    countNewTotalBlock = (size - 1) / BLOCK_SIZE + 1; /* For normal file. */
                                }
                                Debug::debugItem("Stage 3. Remove blocks.");
                                /* Current assume all blocks in local node. */
                                bool found = false;
                                uint64_t i;
                                for (i = 0; i < metaFile.count; i++) {
                                    if ((countTotalBlockTillLastExtentEnd + 1 //metaFile.tuple[i].countExtentBlock 
                                        >= countNewTotalBlock)) {
                                        found = true;
                                        break;
                                    }
                                    countTotalBlockTillLastExtentEnd += 1; //metaFile.tuple[i].countExtentBlock;
                                }
                                if (found == false) {
                                    result = false; /* Fail due to count of current total blocks is less than truncated version. Actually it cannot be reached. */
                                } else {
                                    uint64_t resultFor = true;
                                    // for (uint64_t j = (countNewTotalBlock - countTotalBlockTillLastExtentEnd - 1 + 1); /* Bound of first block to truncate. A -1 is to convert count to bound. A +1 is to move bound of last kept block to bound of first to truncate. */
                                    //     j < metaFile.countExtentBlock[i]; j++) {  /* i is current extent. */ 
                                    //     if (storage->tableBlock->remove(metaFile.indexExtentStartBlock[i] + j) == false) {
                                    //         resultFor = false;
                                    //         break;
                                    //     }
                                    // }
                                    for (int j = metaFile.count; //metaFile.tuple[i].countExtentBlock - 1; 
                                        j >= (int)(countNewTotalBlock - countTotalBlockTillLastExtentEnd - 1 + 1); j--) { /* i is current extent. */ /* Bound of first block to truncate. A -1 is to convert count to bound. A +1 is to move bound of last kept block to bound of first to truncate. */
                                        if (storage->tableBlock->remove(metaFile.BlockList[i].indexMem) == false) {
                                            resultFor = false;
                                            break;
                                        }
                                    }
                                    if (resultFor == false) {
                                        result = false;
                                    } else {
                                        resultFor = true;
                                        // for (uint64_t j = i + 1; j < metaFile.count; j++) { /* Remove rest extents. */
                                        //     for (uint64_t k = 0; k < metaFile.countExtentBlock[j]; k++) {
                                        //         if (storage->tableBlock->remove(metaFile.indexExtentStartBlock[j] + k) == false) {
                                        //             resultFor = false;
                                        //             break;
                                        //         }
                                        //     }
                                        // }
                                        for (uint64_t j = i + 1; j < metaFile.count; j++) { /* Remove rest extents. */
                                            for (int k = metaFile.count - 1; k >= 0; k--) {
                                                if (storage->tableBlock->remove(metaFile.BlockList[j].indexMem) == false) {
                                                    resultFor = false;
                                                    break;
                                                }
                                            }
                                        }
                                        if (resultFor == false) {
                                            result = false; /* Fail due to block remove error. */
                                        } else {
                                            metaFile.size = size; /* Size is the acutal size. */
                                            metaFile.count = i + 1; /* i is the last extent containing last block. */
                                            if (storage->tableFileMeta->put(indexFileMeta, &metaFile) == false) { /* Update meta. */
                                                result = false; /* Fail due to update file meta error. */
                                            } else {
                                                result = true; /* Succeed. */
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Remove directory (Unused).
   @param   path    Path of directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::rmdir(const char *path) 
{
    Debug::debugTitle("FileSystem::rmdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If not directory meta. */
                        result = false;
                    } else {
                        Debug::debugItem("Stage 3. Remove meta from directory.");
                        char *parent = (char *)malloc(strlen(path) + 1);
                        char *name = (char *)malloc(strlen(path) + 1);
                        getNameFromPath(path, name);
                        getParentDirectory(path, parent);
                        if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                            result = false;
                        } else {
                            DirectoryMeta metaDirectory;
                            if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                                result = false; /* Fail due to get file meta error. */
                            } else {
                                if (metaDirectory.count != 0) { /* Directory is not empty. */
                                    result = false; /* Fail due to directory is not empty. */
                                } else {
                                    Debug::debugItem("Stage 3. Remove directory meta.");
                                    if (storage->tableDirectoryMeta->remove(indexDirectoryMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                            }
                        }
			            free(parent);
			            free(name);
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return true;
        }
    }
}

/* Remove file or empty directory. 
   @param   path    Path of file or directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::remove(const char *path, FileMeta *metaFile) 
{
#ifdef TRANSACTION_2PC
    return remove2pc(path, metaFile);
#endif
#ifdef TRANSACTION_CD
    return removecd(path, metaFile);
#endif
}

bool FileSystem::removecd(const char *path, FileMeta *metaFile)
{
    //FileMeta *tmpmetaFile = (FileMeta *)malloc(sizeof(FileMeta)+100);
    Debug::debugTitle("FileSystem::remove");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID = 0;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
		    Debug::debugItem("indexMeta is %ld, isDirectory: %d", (long)indexMeta, (int)isDirectory);
                    char *parent = (char *)malloc(strlen(path) + 1);
                    char *name = (char *)malloc(strlen(path) + 1);
                    DirectoryMeta parentMeta;
	                uint64_t parentHashAddress, parentMetaAddress;
	                uint16_t parentNodeID;
                    getParentDirectory(path, parent);
                    getNameFromPath(path, name);
                    Debug::debugItem("Stage 2. Get meta.");
                    if(isDirectory)
                    {
			Debug::debugItem("Get dir metadata");
                        DirectoryMeta metaDirectory;
                        if (storage->tableDirectoryMeta->get(indexMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            metaFile->count = MAX_FILE_EXTENT_COUNT;
                            if (metaDirectory.count != 0) { /* Directory is not empty. */
                                result = false; /* Fail due to directory is not empty. */
                            } else {
                                // DistributedTxID = TxDistributedBegin();
                                LocalTxID = TxLocalBegin();
                            	if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                            		Debug::notifyError("Remove Meta From Directory failed.");
                                    // TxDistributedPrepare(DistributedTxID, false);
                            		result = false;
                            	} else {
                            		/* Remove meta from directory */
                            		DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
			                        uint64_t indexModifiedNames = 0;
			                        for (uint64_t i = 0; i < parentMeta.count; i++) {
			                            if (strcmp(parentMeta.tuple[i].names, name) == 0) { /* If found selected name. */
			                            } else {
			                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, parentMeta.tuple[i].names); /* Copy original name to modified meta. */
			                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = parentMeta.tuple[i].isDirectories; /* Add directory state. */
			                                indexModifiedNames++;
			                            }
			                        }
			                        metaModifiedDirectory.count = indexModifiedNames;
                                    /* Apply updated data to local log. */
                                    TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                                    /* Receive remote prepare with (OK) */
                                    // TxDistributedPrepare(DistributedTxID, true);
                                    /* Start phase 2, commit it. */
                                    updateRemoteMeta(parentNodeID, &metaModifiedDirectory, parentMetaAddress, parentHashAddress);
                                    /* Only allocate momery, write to log first. */
                            		Debug::debugItem("Stage 3. Remove directory meta.");
	                                if (storage->tableDirectoryMeta->remove(indexMeta) == false) {
	                                    result = false; /* Fail due to remove error. */
	                                } else {
	                                    if (storage->hashtable->del(&hashUnique) == false) {
	                                        result = false; /* Fail due to hash table del. No roll back. */
	                                    } else {
	                                        result = true;
	                                    }
	                                }
                            	}
                                
                            }
                        }   
                    } else {
			Debug::debugItem("Get file metadata");
                        if (storage->tableFileMeta->get(indexMeta, metaFile) == false) {
			    Debug::notifyError("Fail due to get file meta error.");
                            result = false; 
                        } else {
                            // DistributedTxID = TxDistributedBegin();
                            Debug::debugItem("TxLocalBegin ready");
                            LocalTxID = TxLocalBegin();
			    Debug::debugItem("TxLocalBegin");
                        	if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                        		Debug::notifyError("Remove Meta From Directory failed.");
                                // TxDistributedPrepare(DistributedTxID, false);
                        		result = false;
                        	} else {
					Debug::debugItem("Remove meta from directory");
                        		/* Remove meta from directory */
                        		DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
		                        uint64_t indexModifiedNames = 0;
		                        for (uint64_t i = 0; i < parentMeta.count; i++) {
		                            if (strcmp(parentMeta.tuple[i].names, name) == 0) { /* If found selected name. */
		                            } else {
		                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, parentMeta.tuple[i].names); /* Copy original name to modified meta. */
		                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = parentMeta.tuple[i].isDirectories; /* Add directory state. */
		                                indexModifiedNames++;
		                            }
		                        }
		                        metaModifiedDirectory.count = indexModifiedNames;
                                /* Apply updated data to local log. */
                                TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                                /* Receive remote prepare with (OK) */
                                // TxDistributedPrepare(DistributedTxID, true);
                                /* Start phase 2, commit it. */
                                updateRemoteMeta(parentNodeID, &metaModifiedDirectory, parentMetaAddress, parentHashAddress);
                        		/* Only allocate momery, write to log first. */
								bool resultFor = true;
	                            Debug::debugItem("Stage 3. Remove blocks.");
				    for(uint64_t i = 0; i < (metaFile->size / BLOCK_SIZE); i++) {
					/*Get unique hash for each block*/
					char *key = (char *)malloc(sizeof(char) * (strlen(path) + 5));
					sprintf(key, "%s_%d", path, (int) metaFile->BlockList[i].BlockID);
					uint64_t uniqueHashValue = getAddressHash(key);

					if (metaFile->BlockList[i].nodeID == (uint16_t) hashLocalNode) {
					    Debug::debugItem("Stage 4. Remove blocks locally.");
					    if ( (int) metaFile->BlockList[i].tier == 0 ) { /*Remove blocks in Memory tier*/
						storage->extraTableBlock->remove(metaFile->BlockList[i].indexMem);
					    } else { /*Remove blocks in SSD tier*/
 						const char *DBkey = ltos( (long) getAddressHash(key)).data();
						storage->db.remove((const char *)DBkey, sizeof(DBkey));
					    }
					} else {
					    Debug::debugItem("Stage 4. Sent block remove request to remote node");
					    if (!removeRemoteBlock(uniqueHashValue, &metaFile->BlockList[i])) {
						Debug::notifyError("Remove remote block failed.");
					    }
					}
				    }

	                            if (resultFor == false) {
	                                result = false; /* Fail due to block remove error. */
	                            } else {
	                                Debug::debugItem("Stage 4. Remove file meta.");
	                                if (storage->tableFileMeta->remove(indexMeta) == false) {
	                                    result = false; /* Fail due to remove error. */
	                                } else {
	                                    if (storage->hashtable->del(&hashUnique) == false) {
	                                        result = false; /* Fail due to hash table del. No roll back. */
	                                    } else {
	                                        result = true;
	                                    }
	                                }
	                            }
                        	}
                        }
                    }
		            free(parent);
		            free(name);
                }
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::remove2pc(const char *path, FileMeta *metaFile)
{
    Debug::debugTitle("FileSystem::remove");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    char *parent = (char *)malloc(strlen(path) + 1);
                    char *name = (char *)malloc(strlen(path) + 1);
                    getParentDirectory(path, parent);
                    getNameFromPath(path, name);
                    Debug::debugItem("Stage 2. Get meta.");
                    if(isDirectory)
                    {
                        DirectoryMeta metaDirectory;
                        if (storage->tableDirectoryMeta->get(indexMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            metaFile->count = MAX_FILE_EXTENT_COUNT;
                            if (metaDirectory.count != 0) { /* Directory is not empty. */
                                result = false; /* Fail due to directory is not empty. */
                            } else {
                                DistributedTxID = TxDistributedBegin();
                                LocalTxID = TxLocalBegin();
                                if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                                    Debug::notifyError("Remove Meta From Directory failed.");
                                    TxDistributedPrepare(DistributedTxID, false);
                                    result = false;
                                } else {
                                    char *logData = (char *)malloc(strlen(path) + 4);
                                    sprintf(logData, "del %s", path);
                                    /* Apply updated data to local log. */
                                    TxWriteData(LocalTxID, (uint64_t)logData, (uint64_t)strlen(logData));
                                    free(logData);
                                    /* Receive remote prepare with (OK) */
                                    TxDistributedPrepare(DistributedTxID, true);
                                    /* Start phase 2, commit it. */
                                    updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                                    /* Only allocate momery, write to log first. */
                                    Debug::debugItem("Stage 3. Remove directory meta.");
                                    if (storage->tableDirectoryMeta->remove(indexMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                                
                            }
                        }   
                    } else {
                        if (storage->tableFileMeta->get(indexMeta, metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            DistributedTxID = TxDistributedBegin();
                            LocalTxID = TxLocalBegin();
                            if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                                Debug::notifyError("Remove Meta From Directory failed.");
                                TxDistributedPrepare(DistributedTxID, false);
                                result = false;
                            } else {
                                char *logData = (char *)malloc(strlen(path) + 4);
                                sprintf(logData, "del %s", path);
                                /* Apply updated data to local log. */
                                TxWriteData(LocalTxID, (uint64_t)logData, (uint64_t)strlen(logData));
                                free(logData);
                                /* Receive remote prepare with (OK) */
                                TxDistributedPrepare(DistributedTxID, true);
                                /* Start phase 2, commit it. */
                                updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                                /* Only allocate momery, write to log first. */
                                bool resultFor = true;
                                Debug::debugItem("Stage 3. Remove blocks.");
				/*
                                for (uint64_t i = 0; (i < metaFile->count) && (metaFile->tuple[i].hashNode == hashNode); i++) {
                                    for (int j = (int)(metaFile->tuple[i].countExtentBlock) - 1; j >= 0; j--) {
                                        if (storage->tableBlock->remove(metaFile->tuple[i].indexExtentStartBlock + j) == false) {
                                            ;
                                        }
                                    }
                                }
				*/
                                if (resultFor == false) {
                                    result = false; /* Fail due to block remove error. */
                                } else {
                                    Debug::debugItem("Stage 4. Remove file meta.");
                                    if (storage->tableFileMeta->remove(indexMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    free(parent);
                    free(name);
                }
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::blockFree(uint64_t startBlock, uint64_t countBlock)
{
    Debug::debugTitle("FileSystem::blockFree");
    Debug::debugItem("Stage 1. startBlock = %d, countBlock = %d", startBlock, countBlock);
    bool result;
    for (int j = (int)countBlock - 1; j >= 0; j--) {
        if (storage->tableBlock->remove(startBlock + j) == false) {
            result = false; /* Fail due to block remove error. */
            break;
        }
    }
    return result;
}
/* Rename file. 
   @param   pathOld     Old path.
   @param   pathNew     New path.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::rename(const char *pathOld, const char *pathNew) 
{
    Debug::debugTitle("FileSystem::rename");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", pathNew);
    if ((pathOld == NULL) || (pathNew == NULL)) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUniqueOld;
        HashTable::getUniqueHash(pathOld, strlen(pathOld), &hashUniqueOld); /* Get unique hash. */
        NodeHash hashNodeOld = storage->getNodeHash(&hashUniqueOld); /* Get node hash by unique hash. */
        AddressHash hashAddressOld = HashTable::getAddressHash(&hashUniqueOld); /* Get address hash by unique hash. */
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        // UniqueHash hashUniqueNew;
        // HashTable::getUniqueHash(pathNew, strlen(pathNew), &hashUniqueNew); /* Get unique hash. */
        // NodeHash hashNodeNew = storage->getNodeHash(&hashUniqueNew); /* Get node hash by unique hash. */
        // AddressHash hashAddressNew = HashTable::getAddressHash(&hashUniqueNew); /* Get address hash by unique hash. */
        if (checkLocal(hashNodeOld) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNodeOld, hashAddressOld); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUniqueOld, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false; /* Fail due to directory rename is not supported. */
                    } else {
                    	char *parent = (char *)malloc(strlen(pathOld) + 1);
                    	char *name = (char *)malloc(strlen(pathOld) + 1);
                    	getParentDirectory(pathOld, parent);
                    	getNameFromPath(pathOld, name);
                    	if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    		result = false;
                    	} else {
							FileMeta metaFile;
	                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
	                            result = false; /* Fail due to get file meta error. */
	                        } else{
                                updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
	                            Debug::debugItem("Stage 4. Remove file meta.");
	                            if (storage->tableFileMeta->remove(indexFileMeta) == false) {
	                                result = false; /* Fail due to remove error. */
	                            } else {
	                                if (storage->hashtable->del(&hashUniqueOld) == false) {
	                                    result = false; /* Fail due to hash table del. No roll back. */
	                                } else {
	                                    result = true;
	                                }
	                            }
	                        }
                    	}
                    	free(parent);
                    	free(name);
                    }
                }
            }
            unlockWriteHashItem(key, hashNodeOld, hashAddressOld);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Initialize root directory. 
   @param   LocalNode     Local Node ID.
*/
void FileSystem::rootInitialize(NodeHash LocalNode)
{
    this->hashLocalNode = LocalNode; /* Assign local node hash. */
    /* Add root directory. */
    UniqueHash hashUnique;
    HashTable::getUniqueHash("/", strlen("/"), &hashUnique);
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash. */
    Debug::debugItem("root node: %d", (int)hashNode);
    if (hashNode == this->hashLocalNode) { /* Root directory is here. */
        Debug::notifyInfo("Initialize root directory.");
        DirectoryMeta metaDirectory;
        metaDirectory.count = 0;    /* Initialize count of files in root directory is 0. */
        uint64_t indexDirectoryMeta;
        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
            fprintf(stderr, "FileSystem::FileSystem: create directory meta error.\n");
            exit(EXIT_FAILURE);             /* Exit due to parameter error. */
        } else {
            if (storage->hashtable->put("/", indexDirectoryMeta, true) == false) { /* true for directory. */
                fprintf(stderr, "FileSystem::FileSystem: hashtable put error.\n");
                exit(EXIT_FAILURE);             /* Exit due to parameter error. */
            } else {
                ;
            }
        }
    }
}

/*Read extent. That is to parse the part to read in file position information.
   @param   path    Path of file.
   @param   size    Size of data to read.
   @param   offset  Offset of data to read.
   @param   fpi     File position information buffer.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::extentRead(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key) {
    Debug::debugTitle("FileSystem::read");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (fpi == NULL) || (size == 0) || (key == NULL)) { /* Judge if path and file position information buffer are valid or size to read is valid. */
	return false;
    } else {
	UniqueHash hashUnique;
	HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
	    bool result;
            *key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            *key_offset = hashAddress;
	    {
		uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
		    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        Debug::notifyError("Directory meta.");
                        result = false; /* Fail due to not file. */
                    } else {
			FileMeta metaFile;
                        Debug::debugItem("Stage 3. Get Filemeta index.");
                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
                            Debug::notifyError("Fail due to get file meta error.");
                            result = false; /* Fail due to get file meta error. */
                        } else {
			    Debug::debugItem("Stage 3-1. meta.size = %ld, size = %d, offset = %d", (long) metaFile.size, size, offset);
			    if (offset + 1 > metaFile.size) { /* Judge if offset and size are valid. */
                                fpi->len = 0;
                                return true;
                            }
                            else if((metaFile.size - offset) < size)
                            {
                                size = metaFile.size - offset;
                            }

			    /*Locate the right chunk of file //To be implemented.

			    if (metaFile.hasNextChunk) {
				int chunkNum = offset / BLOCK_SIZE;
				FileMeta tmpmetaFile;
				while (chunkNum > 0) {
				    storage->tableFileMeta->get(metaFile->indexOfNextChunk, &tmpmetaFile);
  				    chunkNum--;
				}
			    }*/

			    /*Make sure that all blocks to be read are resides in RDMA region*/
                            uint64_t i;
			    for (i = offset / BLOCK_SIZE; i < (offset + size - 1) / BLOCK_SIZE + 1; i++ ) {

				/*Get unique hash for each block*/
	                        char *key = (char *)malloc(sizeof(char) * (strlen(path) + 5));
        	                sprintf(key, "%s_%d", path, (int)metaFile.BlockList[i].BlockID);
                	        uint64_t uniqueHashValue = getAddressHash(key);
				if (metaFile.BlockList[i].nodeID == (uint16_t)hashLocalNode) {
				    if (!storage->BlockManager->exists(uniqueHashValue)) {
					Debug::debugItem("Fill RDMA region once in local node, Block ID is %d", (int)i);
					//fillRDMARegion(uniqueHashValue, i, &metaFile.BlockList[i], path, false);
                                        /*v2*/
                                        
                                        if (PrefetchManager->find(uniqueHashValue) != PrefetchManager->end()) { //if (Prefetch_stride.previous_blockID == i) {
                                          Debug::debugItem("Block %d has been added to the prefetch queue", i);
                                          while (!storage->BlockManager->exists(uniqueHashValue)) {
                                            ;
                                          }
                                        } else {
                                          fillRDMARegion(uniqueHashValue, i, &metaFile.BlockList[i], path, false);
                                        }
                                        if (storage->BlockManager->exists(uniqueHashValue)) {
                                          Debug::debugItem("Wait and Block %d is prefetched", (int)i);
                                        } else {
                                          Debug::notifyError("Block %d does not exist in RDMA region", (int)i);
                                          return false;
                                        }
                                        
				    } else {
                                        Debug::debugItem("Block %d exists", (int)i);
                                    }
                                    /*Prefetched block must be moved from the prefetch queue.*/
                                    PrefetchManager->erase(uniqueHashValue);
                                    Debug::debugItem("PrefetchManager erase key %s", key);

				} else {
				    Debug::debugItem("Sent block read request to remote node");
				    fillRemoteBlock(uniqueHashValue, &metaFile.BlockList[i], false);
				}
        		    }
			    fillFilePositionInformation(size, offset, fpi, &metaFile);
			    result = true;

                            /*Prefetch*/
                            for (int j = i; j < i + PREFETCHER_NUMBER; j++) {
                              int Prefetch_blockID = j;
                              if (Prefetch_blockID < metaFile.count) {
                                char *Prefetch_key = (char *)malloc(sizeof(char) * (strlen(path) + 5));
                                sprintf(Prefetch_key, "%s_%d", path, (int)metaFile.BlockList[Prefetch_blockID].BlockID);
                                Debug::debugItem("Prefetch_key is %s", Prefetch_key);
                                Debug::debugItem("Current block address is %ld", (long)metaFile.BlockList[Prefetch_blockID].StorageAddress);
                                uint64_t Prefetch_uniqueHashValue = getAddressHash(Prefetch_key);

                                /*If current request has been filled in the prefetch queue, breck to next circle*/
                                if (PrefetchManager->find(Prefetch_uniqueHashValue) != PrefetchManager->end()) {
                                  continue;
                                }
                                PrefetchTask task[PREFETCHER_NUMBER];
                                int taskid = j % PREFETCHER_NUMBER;
                                if (metaFile.BlockList[Prefetch_blockID].nodeID == (uint16_t)hashLocalNode) {
                                  if (!storage->BlockManager->exists(Prefetch_uniqueHashValue)) {
                                    Debug::debugItem("Call preftch thread once\n");
                                    task[taskid].localNode = true;
                                    task[taskid].uniqueHashValue = Prefetch_uniqueHashValue;
                                    task[taskid].blockID = Prefetch_blockID;
                                    task[taskid].block = metaFile.BlockList[Prefetch_blockID];
                                    task[taskid].path = path;
                                    task[taskid].writeOperation = false;
                                    Prefetch_stride.previous_blockID = Prefetch_blockID;
                                    FetchSignal = 0;
                                    Prefetch_queue[taskid].push(&task[taskid]);
                                    PrefetchManager->insert(Prefetch_uniqueHashValue);
                                    Debug::debugItem("Push prefetch request once, BlockID is %d, address is %ld", Prefetch_blockID, (long)task[0].block.StorageAddress);
                                  }
                                }
                              }
                            }
                            /*if ((Prefetch_blockID - Prefetch_stride.previous_blockID) == Prefetch_stride.stride) {
                                  Prefetch_stride.Hitonce = false;
                              } else {
                                  if (Prefetch_stride.Hitonce) {
                                  Prefetch_stride.stride = Prefetch_blockID - Prefetch_stride.previous_blockID;
                                  Prefetch_stride.Hitonce = false;
                                  } else {
                                    Prefetch_stride.Hitonce = true;
                                  }
                              }
                             */

			    /*
			    if(result)
                            {
                                metaFile.timeLastModified = time(NULL);
                                storage->tableFileMeta->put(indexFileMeta, &metaFile);
                                Debug::debugItem("Stage 4, put metaFile to the table");
                            }
			    */
			    Debug::debugItem("Stage end.");
			    return result;
			}
		    }
		}
	    } /*End main part*/
	} /*End if local node*/
    } /*End if path exists*/
    return false;
}


/* Write extent. That is to parse the part to write in file position information. Attention: do not unlock here. Unlock is implemented in updateMeta.
   @param   path        Path of file.
   @param   size        Size of data to write.
   @param   offset      Offset of data to write.
   @param   fpi         File position information.
   @param   metaFile    File meta buffer.
   @param   key         Key buffer to unlock.
   @return              If operation succeeds then return true, otherwise return false.*/
bool FileSystem::extentWrite(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key) {
    Debug::debugTitle("FileSystem::write");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    Debug::debugItem("write, size = %ld, offset = %ld", (long)size, (long)offset);
    if ((path == NULL) || (fpi == NULL) || (key == NULL)) { /* Judge if path, file position information buffer and key buffer are valid. */
        return false;
    }

    /*Initilize data structures*/
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
    bool result;
    *key = lockWriteHashItem(hashNode, hashAddress);  /* Lock hash item. */
    *key_offset = hashAddress;
    uint64_t indexFileMeta;
    bool isDirectory;
    FileMeta *metaFile = (FileMeta *)malloc(sizeof(FileMeta));

    if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
        result = false;     /* Fail due to path does not exist. */
    } else {
        Debug::debugItem("Stage 2. Get meta.");

        if (isDirectory == true) { /* If directory meta. */
            result = false; /* Fail due to not file. */
        } else {
	    if (storage->tableFileMeta->get(indexFileMeta, metaFile) == false) {
                result = false; /* Fail due to get file meta error. */
            } else {
		Debug::debugItem("Stage 3.");

		if ((metaFile->size == 0) || ((offset + size - 1) / BLOCK_SIZE > (metaFile->size - 1) / BLOCK_SIZE)) { /* Judge if new blocks need to be created. */
		    Debug::debugItem("Stage 3-1. Init BlockInfo structure");
		    uint64_t BlockID;
		    uint64_t countExtraBlock; /* Count of extra blocks. At least 1. */

		    /* Consider 0 size file. */
                    Debug::debugItem("Stage 4. metaFile->size = %ld", (long)metaFile->size);
		    if (metaFile->size == 0) {
			BlockID = 0;
			countExtraBlock = (offset + size - 1) / BLOCK_SIZE + 1;
                        Debug::debugItem("Stage 4-1, countExtraBlock = %d, BLOCK_SIZE = %d", countExtraBlock, BLOCK_SIZE);
		    } else {
			BlockID = (int64_t)metaFile->count;
			countExtraBlock = (offset + size - 1) / BLOCK_SIZE - (metaFile->size - 1) / BLOCK_SIZE;
		    }
		    uint64_t indexCurrentExtraBlock;
                    uint64_t indexCurrentMemBlock;
                    Debug::debugItem("Stage 5.");
                    bool resultFor = true;

		    /*Allocate block for the wrtie data*/
                    for (uint64_t i = 0; i < countExtraBlock; i++) {
			Debug::debugItem("for loop, i = %d, BlockID = %d, countExtraBlock = %ld", i, (int)BlockID + 1, (long)countExtraBlock);
			BlockInfo *newBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
			newBlock->BlockID = BlockID;
			newBlock->nodeID = getBlockNodeID();
			newBlock->tier = getBlockTier();

			/*Get unique hash for each block*/
		        char *key = (char *)malloc(sizeof(char) * (strlen(path) + 5));
		        sprintf(key, "%s_%d", path, (int)newBlock->BlockID);
	   		uint64_t uniqueHashValue = getAddressHash(key);
			newBlock->StorageAddress = uniqueHashValue;

			Debug::debugItem("Server nodeID is %d, newBlock->nodeID is %d", (int)hashLocalNode, (int)newBlock->nodeID);
			if (newBlock->nodeID == (uint16_t)hashLocalNode) { /*If new block is allocated in local server*/
			    createNewBlock(newBlock);
			} else {
			    createRemoteBlock(newBlock);
			}
			metaFile->BlockList[BlockID] = *newBlock;
                        metaFile->count++;
                        BlockID ++;
			/*Each FileMeta object contains MAX_FILE_EXTENT_COUNT blocks //To be implemented
			if (metaFile->count >= (MAX_FILE_EXTENT_COUNT - 1)) {
			    FileMeta *tmpmetaFile = (FileMeta *)malloc(sizeof(FileMeta));
			    uint64_t indexOfNextChunk;
			    torage->tableFileMeta->create(&indexOfNextChunk, tmpmetaFile);
			    metaFile->indexOfNextChunk = indexOfNextChunk;
			    metaFile->hasNextChunk = true;
			    storage->tableFileMeta->put(indexFileMeta, metaFile);
			    indexFileMeta = indexOfNextChunk;
			    metaFile = tmpmetaFile;
			}*/
		    }
		    metaFile->size = offset + size;
		    fillFilePositionInformation(size, offset, fpi, metaFile); /* Fill file position information. */
                    result = true;
		} else {
		    /*Fill RDMA Zone*/
                    Debug::debugItem("Stage 3-B. Write data to existing file");
		    metaFile->size = (offset + size) > metaFile->size ? (offset + size) : metaFile->size;
		    /*Make sure that all blocks to be read are resides in RDMA region*/
		    for (uint64_t i = offset / BLOCK_SIZE; i < (offset + size - 1) / BLOCK_SIZE + 1; i++ ) {
			/*Get unique hash for each block*/
                        char *key = (char *)malloc(sizeof(char) * (strlen(path) + 5));
                        sprintf(key, "%s_%d", path, (int)metaFile->BlockList[i].BlockID);
                        uint64_t uniqueHashValue = getAddressHash(key);

			if (metaFile->BlockList[i].nodeID == (uint16_t)hashLocalNode) {
 			    if (!storage->BlockManager->exists(uniqueHashValue)) {
				Debug::debugItem("Fill RDMA region once in local node, Block ID is %d", (int)i);
				fillRDMARegion(uniqueHashValue, i, &metaFile->BlockList[i], path, true);
			    }
			} else {
			    Debug::debugItem("Sent block read request to remote node");
			    fillRemoteBlock(uniqueHashValue, &metaFile->BlockList[i], true);
			    metaFile->BlockList[i].present = true;
			    metaFile->BlockList[i].isDirty = true;
			}
		    }
		    fillFilePositionInformation(size, offset, fpi, metaFile); /* Fill file position information. */
                    result = true;
		} /*End if new blocks need to be created.*/
		if(result) {
		    metaFile->isNewFile = true;
		    metaFile->timeLastModified = time(NULL);
		    storage->tableFileMeta->put(indexFileMeta, metaFile);
		    Debug::debugItem("Stage 5, put metaFile to the table");
		}
		Debug::debugItem("Stage 6. meta.size = %ld",(long) metaFile->size);
                free(metaFile);
                Debug::debugItem("Stage end.");
                return result;
	    } /*End if get file meta succeed*/
        } /*End if not directory meta*/

    } /*End if path exist*/
    return result;
}

/*Get nodeID for newly created block*/
uint16_t FileSystem::getBlockNodeID() {
    uint16_t nodeID = hashLocalNode;
    return nodeID;
}

/*Get storage tier for newly created block*/
uint16_t FileSystem::getBlockTier() {
    uint16_t tier = 0;
    return tier;
}

/*Sent block create request to remote server*/
bool FileSystem::createRemoteBlock(BlockInfo *newBlock) {
    Debug::debugItem("Send Block create request to remote node, nodeid is %d", (int)newBlock->nodeID);
    bool ret = false;
    BlockRequestSendBuffer bufferSend;
    bufferSend.message = MESSAGE_CREATEBLOCK;
    bufferSend.BlockID = newBlock->BlockID;
    bufferSend.Storagetier = newBlock->tier;
    bufferSend.StorageAddress = newBlock->StorageAddress;
    bufferSend.writeOperation = true;
    BlockRequestReceiveBuffer bufferReceive;
    RdmaCall(newBlock->nodeID, (char *)&bufferSend, (uint64_t)sizeof(BlockRequestSendBuffer), (char *)&bufferReceive, (uint64_t)sizeof(BlockRequestReceiveBuffer));
    if (bufferReceive.result == false) {
	Debug::notifyError("Remote Call on CreateBlock With Error.");
    } else {
	Debug::debugItem("Block is created in remote node");
	newBlock->indexCache = bufferReceive.indexCache;
	newBlock->indexMem = bufferReceive.indexMem;
	newBlock->StorageAddress = bufferReceive.StorageAddress;
	ret = true;
    }
    return ret;
}

/*Sent block remove request to remote server*/
bool FileSystem::removeRemoteBlock(uint64_t uniqueHashValue, BlockInfo *newBlock) {
    Debug::debugItem("Send Block remove request to remote node, nodeid is %d", (int)newBlock->nodeID);
    bool ret = false;
    BlockRequestSendBuffer bufferSend;
    bufferSend.message = MESSAGE_REMOVEBLOCK;
    bufferSend.uniqueHashValue = uniqueHashValue;
    bufferSend.BlockID = newBlock->BlockID;
    bufferSend.Storagetier = newBlock->tier;
    bufferSend.StorageAddress = newBlock->StorageAddress;
    bufferSend.writeOperation = true;
    BlockRequestReceiveBuffer bufferReceive;
    RdmaCall(newBlock->nodeID, (char *)&bufferSend, (uint64_t)sizeof(BlockRequestSendBuffer), (char *)&bufferReceive, (uint64_t)sizeof(BlockRequestReceiveBuffer));
    if (bufferReceive.result == false) {
        Debug::notifyError("Remote Call on CreateBlock With Error.");
    } else {
	Debug::debugItem("Block is removed in remote node");
	ret = true;
    }
    return ret;
}

/*Remove block for remote request*/
bool FileSystem::removeBlock(uint64_t uniqueHashValue, uint16_t tier, uint64_t StorageAddress) {
    bool ret = false;
    uint64_t MemZoneBaseAddress = server->getMemoryManagerInstance()->getExtraDataAddress();
    Debug::debugItem("Move data to RDMA region for remote remove");
    /*To be implemented*/
    if ((int)tier == 0) {
	int bid = (StorageAddress - MemZoneBaseAddress) / BLOCK_SIZE;
	storage->extraTableBlock->remove(bid);
        ret = true;
    } else {
	const char *key = ltos((long)uniqueHashValue).data();
        storage->db.remove(key, sizeof(key));
	ret = true;
    }
    return ret;
}

/*Create a new block*/
bool FileSystem::createNewBlock(BlockInfo *newBlock) {
    bool ret = false;
    uint64_t indexCurrentExtraBlock;
    uint64_t indexCurrentMemBlock;
    uint64_t uniqueHashValue = newBlock->StorageAddress;
    Debug::debugItem("Create a new block");
    if (newBlock->tier == 0) { /*If this block is allocated in memory storage tier*/
        Debug::debugItem("Memory storage tier");
	uint64_t MemZoneBaseAddress = server->getMemoryManagerInstance()->getExtraDataAddress();
	if ((storage->tableBlock->create(&indexCurrentExtraBlock) == false) || (storage->extraTableBlock->create(&indexCurrentMemBlock) == false)) {
	    Debug::notifyError("Allocate blcok Error");
            ret = false; /* Fail due to no enough space. Might cause inconsistency. */
	} else { /* So we need to modify the allocation way in table class. */
	    newBlock->indexCache = indexCurrentExtraBlock;
	    newBlock->indexMem = indexCurrentMemBlock;
	    newBlock->StorageAddress = MemZoneBaseAddress + indexCurrentMemBlock * BLOCK_SIZE;
	    newBlock->isDirty = true;
	    newBlock->present = true;
	}
	ret = true;
    } else if (newBlock->tier == 1) {
	Debug::debugItem("SSD storage tier");
	if (storage->tableBlock->create(&indexCurrentExtraBlock) == false) {
            Debug::notifyError("Allocate blcok Error");
            ret = false; /* Fail due to no enough space. Might cause inconsistency. */
        } else {
	    newBlock->indexCache = indexCurrentExtraBlock;
	    newBlock->indexMem = -1;
	    newBlock->isDirty = true;
            newBlock->present = true;
	}
	ret = true;
    } /*End if 'tier == 0' */
    LRUInsert(uniqueHashValue, newBlock);
    Debug::debugItem("Block created!");
    return ret;
}

/*Sent fill block request to remote node*/
bool FileSystem::fillRemoteBlock(uint64_t uniqueHashValue, BlockInfo *newBlock, bool writeOperation) {
    Debug::debugItem("Send Block read request to remote node, nodeid is %d", (int)newBlock->nodeID);
    bool ret = false;
    BlockRequestSendBuffer bufferSend;
    bufferSend.message = MESSAGE_READBLOCK;
    bufferSend.uniqueHashValue = uniqueHashValue;
    bufferSend.BlockID = newBlock->BlockID;
    bufferSend.Storagetier = newBlock->tier;
    bufferSend.StorageAddress = newBlock->StorageAddress;
    bufferSend.writeOperation = writeOperation;
    BlockRequestReceiveBuffer bufferReceive;
    RdmaCall(newBlock->nodeID, (char *)&bufferSend, (uint64_t)sizeof(BlockRequestSendBuffer), (char *)&bufferReceive, (uint64_t)sizeof(BlockRequestReceiveBuffer));
    if (bufferReceive.result == false) {
        Debug::notifyError("Remote Call on ReadBlock With Error.");
    } else {
        Debug::debugItem("Block is prepared to be read in remote node");
        newBlock->indexCache = bufferReceive.indexCache;
        ret = true;
    }
    return ret;
}

/*long to string*/
std::string FileSystem::ltos(long l) {
    std::ostringstream os;
    os << l;
    std::string result;
    std::istringstream is(os.str());
    is >> result;
    return result;
}

/*Get hash value of the input path*/
uint64_t FileSystem::getAddressHash(char *path) {
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique);
    return hashUnique.value[3];
}

/*Insert a block to BlockManager, and repalce an obsolete block with LRU strategy*/
bool FileSystem::LRUInsert(uint64_t key, BlockInfo *newBlock) {
    Debug::debugItem("LRUInsert:: Call LRUInsert once");
    BlockInfo *oldBlock = (BlockInfo *)malloc(sizeof(BlockInfo));
    *oldBlock = storage->BlockManager->put(key, *newBlock);

    /*Evict an obsolete block*/
    if ((long)oldBlock->StorageAddress != 0L) {
	Debug::debugItem("LRUInsert:: Evict one block with LRU strategy, BlockID is %d", oldBlock->BlockID);
	/*If block is clean, remove the block in memory and return*/
	if (!oldBlock->isDirty) {
	    storage->tableBlock->remove(oldBlock->indexCache);
	    return true;
	}
	/*If block is dirty, copy data to memory tier or SSD tier*/
	if (oldBlock->tier == 0) {
	    uint64_t MemZoneBaseAddress = server->getMemoryManagerInstance()->getExtraDataAddress();
	    uint64_t RdmaZoneBaseAddress = server->getMemoryManagerInstance()->getDataAddress();
	    void *dest = (void *) (oldBlock->StorageAddress);
	    void *src  = (void *) (RdmaZoneBaseAddress + oldBlock->indexCache * BLOCK_SIZE);
            Debug::debugItem("LRUInsert:: src is %ld, dest is %ld", (long)src, (long)dest);
	    memcpy(dest, src, BLOCK_SIZE);
	    Debug::debugItem("LRUInsert:: Dirty data have been moved to the memory tier");
	} else if (oldBlock->tier == 1) {
	    uint64_t RdmaZoneBaseAddress = server->getMemoryManagerInstance()->getDataAddress();
	    const char *key = ltos((long)oldBlock->StorageAddress).data();
	    void *value  = (void *) (RdmaZoneBaseAddress + oldBlock->indexCache * BLOCK_SIZE);
	    storage->db.set((const char *)key, sizeof(key), (const char *)value, BLOCK_SIZE);
	    Debug::debugItem("LRUInsert:: Dirty data have been moved to the SSD tier");
	}
	storage->tableBlock->remove(oldBlock->indexCache);
    }
    return true;
}

/*Prefetch Task*/
bool FileSystem::PrefetcherWorker(int id) {
  PrefetchTask *task;
  while (true) {
    task = Prefetch_queue[id].pop();
    Debug::debugItem("Pop prefetch request once from Prefetch_queue %d", id);
    if (task->localNode) {
      Debug::debugItem("Prefetch Block %d, address is %ld", task->blockID, (long)task->block.StorageAddress);
      if (!storage->BlockManager->exists(task->uniqueHashValue)) {
        fillRDMARegion(task->uniqueHashValue, task->blockID, &task->block, task->path, task->writeOperation);
        __sync_fetch_and_add(&FetchSignal, 1);
      }
    } else {
      Debug::debugItem("Prefetch Block %d, Not local node", task->blockID);
    }
  }
  return true;
}

/* Constructor of file system. 
   @param   buffer              Buffer of memory.
   @param   countFile           Max count of file.
   @param   countDirectory      Max count of directory.
   @param   countBlock          Max count of blocks. 
   @param   countNode           Max count of nodes.
   @param   hashLocalNode       Local node hash. From 1 to countNode. */
FileSystem::FileSystem(char *buffer, char *bufferBlock, char *extraBlock, uint64_t countFile,
                       uint64_t countDirectory, uint64_t countBlock, 
                       uint64_t countNode, NodeHash hashLocalNode)
{
    if ((buffer == NULL) || (bufferBlock == NULL) || (countFile == 0) || (countDirectory == 0) ||
        (countBlock == 0) || (countNode == 0) || (hashLocalNode < 1) || 
        (hashLocalNode > countNode)) {
        fprintf(stderr, "FileSystem::FileSystem: parameter error.\n");
	fprintf(stderr, "hashLocalNode = %d\n", (int)hashLocalNode);
        exit(EXIT_FAILURE);             /* Exit due to parameter error. */
    } else {
        this->addressHashTable = (uint64_t)buffer;
        storage = new Storage(buffer, bufferBlock, extraBlock, countFile, countDirectory, countBlock, countNode); /* Initialize storage instance. */
	printf("Debug-FileSystem.cpp: Storage init done\n");
        lock = new LockService((uint64_t)buffer);
	printf("Debug-FileSystem.cpp: lock service done\n");
    }
    //uint64_t RdmaBlockCount = RDMA_DATASIZE * 1024 * 1024 / BLOCK_SIZE;
    //BlockManager = new cache::lru_cache<uint64_t, BlockInfo>(RdmaBlockCount);
    PrefetchManager = new std::unordered_set<uint64_t>(64);
    for (int i = 0; i < PREFETCHER_NUMBER; i++) {
      Prefecther[i] = thread(&FileSystem::PrefetcherWorker, this, i);
    }
    Debug::debugItem("FileSystem:: Init prefetch thread");
    Prefetch_stride.previous_blockID = -1;
    Prefetch_stride.stride = 1;
    Prefetch_stride.Hitonce = false;
}
/* Destructor of file system. */
FileSystem::~FileSystem()
{
    delete storage;                     /* Release storage instance. */
    Prefecther[0].detach();
}
