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
    delete hashTable; // Also delete the hash table
}


//----------------------------------------
// `allocBuf`
// This is the CLOCK algorithm.
// It finds a free frame to use.
//----------------------------------------
const Status BufMgr::allocBuf(int & frame) 
{
    // We will scan the buffer pool up to two full circles
    // (numBufs * 2). If we go around twice and find nothing,
    // it means every single frame is pinned.
    for (int i = 0; i < numBufs * 2; i++)
    {
        // 1. Advance Clock Pointer (as per Figure 2)
        advanceClock(); // This just does clockHand = (clockHand + 1) % numBufs;

        BufDesc* currentFrame = &bufTable[clockHand];

        // 2. Check "Valid Set?" (from Figure 2)
        // If the frame is not valid, it's empty! This is the
        // best and easiest case.
        if (!currentFrame->valid)
        {
            frame = clockHand; // Return the frame number by reference
            return OK;
        }

        // 3. Check "refBit Set?" (from Figure 2)
        if (currentFrame->refbit)
        {
            // The page was recently used. Give it a "second chance."
            // "Clear refBit" (from Figure 2)
            currentFrame->refbit = false;
            // And continue to the next frame in our loop
            continue; 
        }

        // 4. Check "Page Pinned?" (from Figure 2)
        // If refbit is false, we check the pin count.
        if (currentFrame->pinCnt > 0)
        {
            // The page is currently in use. We cannot evict it.
            // We just "continue" to the next frame, leaving its
            // refbit as 'false'.
            continue;
        }

        // 5. This is our VICTIM!
        // If we reach this point, it means:
        // - currentFrame->valid == true
        // - currentFrame->refbit == false
        // - currentFrame->pinCnt == 0
        // This is the frame we will replace.

        // 6. Check "Dirty Bit Set?" (from Figure 2)
        if (currentFrame->dirty)
        {
            // "Flush Page to Disk" (from Figure 2)
            // We MUST write the changes back to disk before evicting.
            Status status = currentFrame->file->writePage(currentFrame->pageNo, &bufPool[clockHand]);
            
            // If the disk write fails, we must return an error.
            if (status != OK)
            {
                return UNIXERR; 
            }
            // The page is now "clean" since it's saved to disk.
            currentFrame->dirty = false;
        }

        // 7. Prepare the frame for its new user.
        // We must remove the *old* page's entry from the hash table.
        // This is part of "Invoke Set() on Frame"
        hashTable->remove(currentFrame->file, currentFrame->pageNo);

        // We have found our frame.
        frame = clockHand; // Return the frame number
        return OK;
    }

    // If we get here, we looped (numBufs * 2) times and found
    // no frame. This means all frames are pinned.
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // -- Morgan (Person 2) was here. Test push! --
    
    //access counter for statistics
    bufStats.accesses++; //bufStats (lowercase) is the variable

    //spot number on RAM
    int frameNo;
    Status status;

    //check hashtable if page is already in RAM
    status = hashTable->lookup(file, PageNo, frameNo); 

    if(status == OK){
        //cache hit
        //page is in buffer
        //page recently used
        bufTable[frameNo].refbit = true; 

        //clock algorithm: page in use, dont evict
        bufTable[frameNo].pinCnt++; 

        //page output parameter point to fram in buffer
        page = &bufPool[frameNo]; 
        return OK;
    }
    else if (status == HASHNOTFOUND){
        //cache miss, read disk

        //disk read statistics
        bufStats.diskreads++;

        //2B1
        //find free frame
        status = allocBuf(frameNo); 
        if (status != OK){
            return status; //all buffer in use, bufferexceed
        }

        //2B2
        //have free frame now
        status = file->readPage(PageNo, &bufPool[frameNo]); 
        if (status != OK){
            //disk read failed (page doesn't exist).
            return UNIXERR;
        }

        //2B3
        //this page lives in this frame, log
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK){
            
            return HASHTBLERROR;
        }

        //2B4
        //setup frame metadata
        //Set() helper function initializes the frame's state
        bufTable[frameNo].Set(file, PageNo); 

        //2B5
        //return pointer set page outpit paremeter like in hit case
        page = &bufPool[frameNo]; 

        return OK;
    }
    else{
        //2C error occured
        return status;
    }
}

//----------------------------------------
// `unPinPage`
// This tells the buffer manager we are
// done using a page.
//----------------------------------------
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    // 1. Find the frame for the page we are unpinning.
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK)
    {
        // The page isn't in the buffer. This is an error.
        return HASHNOTFOUND;
    }

    BufDesc* currentFrame = &bufTable[frameNo];

    // 2. Check if the pin count is already 0.
    if (currentFrame->pinCnt == 0)
    {
        // This is an error. We can't unpin a page that
        // isn't pinned.
        return PAGENOTPINNED;
    }

    // 3. Decrement the pin count.
    // This signals that one user is done with the page.
    currentFrame->pinCnt--;

    // 4. Check if the page was modified (is "dirty").
    if (dirty)
    {
        // The user is telling us they modified this page.
        // We MUST set the dirty bit to 'true'.
        currentFrame->dirty = true;
    }

    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status;
    int newPageNumber;
    int allocatedFrameNumber;

    //1 allocate new empty page on disk
    status = file->allocatePage(newPageNumber);
    if (status != OK){
        return UNIXERR; //fail if disk is full
    }

    //disk read counter
    bufStats.diskreads++;

    //2 find free frame in buffer pool
    status = allocBuf(allocatedFrameNumber);
    if (status != OK){
        //buffer pool is full of pinned pages.
        //just allocated a page on disk, but we can't bring it into memory
        //return the error
        return status;
    }

    //3 map new page to its fram in the has table
    status = hashTable->insert(file, newPageNumber, allocatedFrameNumber);
    if (status != OK){
        return HASHTBLERROR;
    }

    //4 initialize new frame metadata
    //set pinCnt=1, dirty=false, valid=true
    bufTable[allocatedFrameNumber].Set(file, newPageNumber);

    //5 retrn new page number and pointer
    pageNo = newPageNumber;
    page = &bufPool[allocatedFrameNumber];

    return OK;
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


