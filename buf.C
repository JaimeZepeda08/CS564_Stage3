#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
* This function allocates a buffer frame using the clock replacement policy
* Returns:
*   OK if successful
*   BUFFEREXCEEDED if all buffer frames are pinned
*/
const Status BufMgr::allocBuf(int& frame) 
{
    // loop through buffer pool looking for a free frame
    // we can scan the buffer pool at most 2 times (worst case) this means
    // that all buffers are pinned so there arent any free buffers
    for (int i = 0; i < numBufs*2; i++) {
        // advance the clock hand
        advanceClock();

        // get frame at the current clock hand position
        BufDesc* currFrame = &bufTable[clockHand];
        
        // check if valid bit is set
        if (currFrame->valid == false) {
            // found a free frame
            frame = clockHand;
            return OK;
        }

        // check if reference bit is set
        if (currFrame->refbit == true) {
            // clear reference bit and continue
            // this frame will be checked again in the next pass (if needed)
            currFrame->refbit = false;
            continue;
        }

        // check if frame is pinned
        if (currFrame->pinCnt > 0) {
            // advance clock hand and continue
            continue;
        }

        // check dirty bit
        if (currFrame->dirty == true) {
            #ifdef DEBUGBUF
            cout << "flushing page " << currFrame->pageNo
                    << " from frame " << clockHand << endl;
            #endif

            Status status = currFrame->file->writePage(currFrame->pageNo,
                                                  &(bufPool[clockHand]));
            if (status != OK) {
                return UNIXERR;
            }
    
            currFrame->dirty = false;
        }

        // remove from hash table
        Status status = hashTable->remove(currFrame->file,
                                            currFrame->pageNo);
        if (status != OK) {
            return UNIXERR;
        }

        // if we reached here, then we found a free frame
        frame = clockHand;
        return OK;
    }

    // we scanned buffer pool twice, buffer is full
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{





}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


