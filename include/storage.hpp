/*** Storage header. ***/

/** Version 2 + functional model modifications. **/

/** Redundance check. **/
#ifndef STORAGE_HEADER
#define STORAGE_HEADER

/** Included files. **/
#include <stdint.h>                     /* Standard integers. E.g. uint16_t */
#include "hashtable.hpp"                /* Hash table class. */
#include "table.hpp"                    /* Table template. */
#include "global.h"
#include "kcdirdb.h"
#include "kcdirdb.h"
#include "lrucache.hpp"

typedef struct                          /* Block structure. */
{
    char bytes[BLOCK_SIZE];             /* Raw data. */
} Block;
/* Currently here is no back pointer in the block structure, which means consistency might be
   weak. If a meta is removed but the related blocks are not, then it will cost a lot of time
   to scan all files to determine which blocks need to be removed (E.g. rebuild a new bitmap
   from meta to represent latest blocks information and then compare it with the original 
   block bitmap and fix). Besides, there is no checksum here, data correctness cannot be 
   determined. */

class Storage
{
private:
    uint64_t countNode;                 /* Count of nodes. */

public:
    uint64_t sizeBufferUsed;            /* Size of used bytes in buffer. */
    HashTable *hashtable;               /* Hash table. */
    Table<FileMeta> *tableFileMeta;     /* File meta table. */
    Table<DirectoryMeta> *tableDirectoryMeta; /* Directory meta table. */
    Table<Block> *tableBlock;           /* Block table. */
    Table<Block> *extraTableBlock;      /*Extra Block table*/
    NodeHash getNodeHash(UniqueHash *hashUnique); /* Get node hash by unique hash. */

    kyotocabinet::DirDB db;
    cache::lru_cache<uint64_t, BlockInfo> *BlockManager;
    //NodeHash getNodeHash(const char *buffer); /* Get node hash. */
    Storage(char *buffer, char *bufferBlock, char *extraBlock, uint64_t countFile, uint64_t countDirectory, uint64_t countBlock, uint64_t countNode); /* Constructor. */
    ~Storage();                         /* Deconstructor. */
};

/** Redundance check. **/
#endif
