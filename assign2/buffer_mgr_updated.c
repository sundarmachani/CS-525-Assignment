#include "dt.h"
#include "buffer_mgr.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <limits.h>
#include "storage_mgr.h"

typedef struct BufferPoolFrame
{
    int indexpool;            
    int indexpage;                
    struct BufferPoolFrame *nextframe;  
    struct BufferPoolFrame *prevframe;  
} BufferPoolFrame;

typedef struct BPData
{
    int pageframesavailable;      
    int readoperations;                  
    int writeoperations;                 

 
    int *listPageNo;
    int *fixcounts;
    bool *dirtyflag;

  
    char *BpoolData;

    // Linked list for BufferPoolFrame
    BufferPoolFrame *headframe;  
    BufferPoolFrame *currentframe; 
    BufferPoolFrame *endframe;   
} BPData;


// Struct to maintain page access history
typedef struct PageAccessHistory
{
    int *accessTimes; // Store the last K access times
    int count;        // no. of times the page was accessed
} PageAccessHistory;


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle);
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
RC closePageFile(SM_FileHandle *fHandle);
void LRUCachePinPage(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum);
int LRUpinPageFIFO(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum, SM_FileHandle *fHandle);
static PageNumber findPageInBuffer(BPData *bufferPoolData, PageNumber thepage, int numPages);

/**
 * This Function initalizes the buffer pool, then allocates memory
 */
RC initBPD(BPData *bpdata, int numPages)
{
    bpdata->dirtyflag = malloc(numPages * sizeof(bool));
    bpdata->listPageNo = malloc(numPages * sizeof(int));
    bpdata->BpoolData = calloc(numPages * PAGE_SIZE, sizeof(char));
    bpdata->pageframesavailable = numPages;
    bpdata->fixcounts = calloc(numPages, sizeof(int));

    if (!bpdata->dirtyflag ||
        !bpdata->listPageNo || !bpdata->BpoolData ||
        !bpdata->fixcounts)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    // init
    bpdata->headframe = NULL;
    bpdata->currentframe = NULL;
    bpdata->endframe = NULL;
    bpdata->readoperations = 0;
    bpdata->writeoperations = 0;

    for (int index = 0; index < numPages; index++)
    {
        bpdata->listPageNo[index] = NO_PAGE;
        bpdata->dirtyflag[index] = false;
    }

    return RC_OK;
}

/**
 * Initializes the buffer pool, then allocates memory. Checks if the page file name exists,
 * whether pool has initialized or not.
 */RC initBP(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    if (!bm || !pageFileName) return RC_NULL_PARAM;

    if (stat(pageFileName, &(struct stat){}) != 0) return RC_FILE_NOT_FOUND;

    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    if (!(bm->mgmtData = calloc(1, sizeof(BPData)))) return RC_MEM_ALLOC_FAILURE;

    if (initializeBufferPoolData((BPData *)bm->mgmtData, numPages) != RC_OK) {
        free(bm->mgmtData);
        return RC_MEM_ALLOC_FAILURE;
    }

    return RC_OK;
}

/**
 * Checks for pinned pages
 */
bool hasPinnedPages(const BPData *bpdta, int numPages) {
    for (int index = 0; index < numPages; index++) {
        if (bpdta->fixcounts[index] > 0) {
            return true; // Found pinned page
        }
    }
    return false; // none found
}

/**
 * Frees all allocated memory for the buffer pool data.
 */
void freeBufferPoolData(BPData *bpdta) {
    // Free allocated memory
    free(bpdta->listPageNo);
    free(bpdta->fixcounts);
    free(bpdta->BpoolData);
    free(bpdta->dirtyflag);

    // Reset pointers=
    void **pointers[] = {
        (void **)&bpdta->listPageNo,
        (void **)&bpdta->fixcounts,
        (void **)&bpdta->BpoolData,
        (void **)&bpdta->dirtyflag
    };

    for (int i = 0; i < sizeof(pointers) / sizeof(pointers[0]); i++) {
        *pointers[i] = NULL;
    }

    // Reset linked list pointers
    bpdta->headframe = NULL;
    bpdta->currentframe = NULL;
    bpdta->endframe = NULL;

    // Reset op counters
    bpdta->readoperations = 0;
    bpdta->writeoperations = 0;
}



/**
 * Shut down buffer pool and flush dirty pages
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (!bm) {
        return RC_BUFFER_POOL_NOT_EXIST;
    }

    BPData *bpdta = (BPData *)bm->mgmtData;
    if (!bpdta) {
        return RC_BUFFER_POOL_DATA_NOT_EXIST;
    }

    if (hasPinnedPages(bpdta, bm->numPages)) {
        return RC_SHUTDOWN_POOL_ERROR;
    }

    forceFlushPool(bm);
    freeBufferPoolData(bpdta);
    free(bm->mgmtData);
    bm->mgmtData = NULL;

    return RC_OK;
}

/**
 * Forcecully flush the buffer pool, write all dirty pages by iterating over each page in the buffer pool.
 * If dirty and not fixed, write to disk. If not, reset the dirty flag.
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
    SM_FileHandle fileHandle;
    int status = openPageFile(bm->pageFile, &fileHandle);

    if (status != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }

    BPData *bpdta = (BPData *)bm->mgmtData;

    // Iterate over each page in the buffer pool
    for (PageNumber pageNumber = 0; pageNumber < bm->numPages; pageNumber++)
    {
        // Check if the page is dirty - modified - and not fixed - pinned
        if (bpdta->dirtyflag[pageNumber] && bpdta->fixcounts[pageNumber] == 0)
        {
            SM_PageHandle pageHandle = bpdta->BpoolData + pageNumber * PAGE_SIZE * sizeof(char);
            int actualPageIndex = bpdta->listPageNo[pageNumber];
            writeBlock(actualPageIndex, &fileHandle, pageHandle);
            bpdta->dirtyflag[pageNumber] = false;
            bpdta->writeoperations++;
        }
    }

    // Close page file
    closePageFile(&fileHandle);
}

/**
 * Mark page in buffer pool as dirty. The function takes the buffer pool, points to the page handle struct and the page no.
 * then returns a success code if found. 
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    /* Checks if the buffer pool or page handle is NULL. */
    if (bm == NULL || page == NULL)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }
    BPData *bpdta = (BPData *)bm->mgmtData;
    PageNumber thepage = page->pageNum;

    /* Find the page number in the buffer pool. */
    PageNumber bufferPoolPageNumber = findPageInBuffer(bpdta, thepage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    bpdta->dirtyflag[bufferPoolPageNumber] = true;
    return RC_OK;
}

/**
 * The function looks for a page number in the buffer pool, takes the buffer pool data structure, the targer page no., 
 * the total no. of pages, and returns the index of the pool where the page is found. or NO_PAGE if not found. 
 */
static PageNumber findPageInBuffer(BPData *bpdta, PageNumber thepage, int numPages)
{
    BufferPoolFrame *thisPage = bpdta->headframe;

    // Calculate the number of filled slots in the buffer pool
    int fullslots = numPages - bpdta->pageframesavailable;

    // Traverse the linked list
    for (int i = 0; i < fullslots && thisPage != NULL; i++)
    {
        if (thisPage->indexpage == thepage)
        {
            return thisPage->indexpool;
        }
        thisPage = thisPage->nextframe;
    }
    return NO_PAGE;
}

/**
 * This function is used to unpin a page in the buffer pool. When a page is unpinned,
 * its fix count is decremented by 1. If the fix count of the page becomes 0, it means
 * that the page is no longer fixed and can be evicted from the buffer pool if necessary.
 */
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm == NULL || page == NULL)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    BPData *bpdta = (BPData *)bm->mgmtData;
    PageNumber thepage = page->pageNum;

    PageNumber bufferPoolPageNumber = findPageInBuffer(bpdta, thepage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    if (bpdta->fixcounts[bufferPoolPageNumber] > 0)
    {
        bpdta->fixcounts[bufferPoolPageNumber]--;
    }

    return RC_OK;
}

/**
 * This function writes a page to disk by opening the page file, writing the page data
 * to disk using the page file and the actual page number, and then closing the page file.
 */
RC writePageToFile(const char *pageFile, PageNumber pageNum, SM_PageHandle pageHandle)
{
    // Open the page file for writing
    SM_FileHandle fileHandle;
    RC status = openPageFile(pageFile, &fileHandle);
    if (status != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Write the page data to the file using the page file and the actual page number
    status = writeBlock(pageNum, &fileHandle, pageHandle);

    // Close the page file
    closePageFile(&fileHandle);
    return status;
}

/**
 * This writes the page data to disk and marks the page as no longer dirty.
 * If the page is found, it writes the page data to disk using the page file and the actual page no.
 * Then, it resets the dirty flag for the page in the buffer pool data and increments the write op counter.
 * Then returns the success RC_OK code. 
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Get the buffer pool data
    BPData *bufferPoolData = (BPData *)bm->mgmtData;
    // Get the actual page number from the page handle
    PageNumber actualPageNumber = page->pageNum;
    // Find the index of the page in the buffer pool
    PageNumber bufferPoolPageNumber = findPageInBuffer(bufferPoolData, actualPageNumber, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }
    RC status = writePageToFile(bm->pageFile, actualPageNumber, page->data);
    if (status != RC_OK)
    {
        return status;
    }

    // Mark the page as not dirty
    bufferPoolData->dirtyflag[bufferPoolPageNumber] = false;
    // Increment write operations counter
    bufferPoolData->writeoperations++;
    return RC_OK;
}

// Helper function to get buffer pool data
static inline BPData *getBufferPoolData(BM_BufferPool *const bm)
{
    return (BPData *)bm->mgmtData;
}

/**
 * Returns an array of PageNumber containing the page numbers of all the frames in the buffer pool.
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->listPageNo;
}

/**
 * Returns an array of of bools that indicate whether each page in the buffer pool is dirty or not.
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->dirtyflag;
}

/**
 * Returns an array of integers showing the number of times each page in the buffer pool
 * has been fixed 
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->fixcounts;
}


int getNumWriteIO(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->writeoperations;
}

/**
 * Finds the page number of a page in the cache.
 */
static bool findPageInCache(BPData *bpd, PageNumber pageNum, PageNumber *pgindexbp)
{
    for (BufferPoolFrame *temp = bpd->headframe; temp != NULL; temp = temp->nextframe)
    {
        if (temp->indexpage == pageNum)
        {
            *pgindexbp = temp->indexpool;
            return true;
        }
    }
    return false;
}

/**
 * Adds a new frame to the cache.
 */
static void addNewFrameToCache(BPData *bpdt, PageNumber pageNum, PageNumber pgindexbp) {
    BufferPoolFrame *pghandle = malloc(sizeof(BufferPoolFrame));
    pghandle->indexpool = pgindexbp;
    pghandle->indexpage = pageNum;
    pghandle->nextframe = NULL;

    if (!bpdt->headframe) {
        bpdt->headframe = bpdt->endframe = pghandle;
        pghandle->prevframe = NULL;
    } else {
        bpdt->endframe->nextframe = pghandle;
        pghandle->prevframe = bpdt->endframe;
        bpdt->endframe = pghandle;
    }

    bpdt->listPageNo[pgindexbp] = pageNum;
    bpdt->pageframesavailable--;
}

/**
 * This function gets a page from the buffer pool for the given page number. If the page is already there,
 * it brings it to front. If not, it selects a new frame and that page is read from disk to the frame.
 * The frame is then added to the cache.
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Open page file
    SM_FileHandle sm_fileHandle;
    RC status = openPageFile(bm->pageFile, &sm_fileHandle);
    if (status != RC_OK)
        return RC_FILE_NOT_FOUND;

    // If the page file is smaller than the req page number, make sure it has enough space
    if (sm_fileHandle.totalNumPages <= pageNum)
    {
        ensureCapacity(pageNum + 1, &sm_fileHandle);
    }

    page->pageNum = pageNum;
    BPData *bpdt = (BPData *)bm->mgmtData;
    PageNumber pgindexbp = NO_PAGE;
    bool isPageInCache = findPageInCache(bpdt, pageNum, &pgindexbp);


    size_t charSize = sizeof(char);

    // If the page is in the cache, bring it to the front, update page handle
    if (isPageInCache)
    {
        // If the buffer pool uses LRU, update the page's position in cache
        if (bm->strategy == RS_LRU)
        {
            LRUCachePinPage(bm, page, pageNum);
        }
        page->data = bpdt->BpoolData + pgindexbp * PAGE_SIZE * charSize;
        bpdt->fixcounts[pgindexbp]++;
    }
    else
    {
        // If the page is not in cache, try to find a frame to add it to
        if (bpdt->pageframesavailable > 0)
        {
            pgindexbp = bm->numPages - bpdt->pageframesavailable;
            addNewFrameToCache(bpdt, pageNum, pgindexbp);
        }
        else
        {
            // If there is no space in the buffer pool, select a frame based on the buffer pool strategy
            if (bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
            {
                pgindexbp = LRUpinPageFIFO(bm, page, pageNum, &sm_fileHandle);
            }
            else if (bm->strategy == RS_LRU_K)
            {
                printf("Not implemented yet\n");
            }
    
        }

        page->data = bpdt->BpoolData + pgindexbp * PAGE_SIZE * charSize;
        bpdt->fixcounts[pgindexbp]++;

        // Only read from disk if the page is not in cache
        readBlock(page->pageNum, &sm_fileHandle, page->data);
        bpdt->readoperations++;
    }

    closePageFile(&sm_fileHandle);
    return RC_OK;
}


void LRUCachePinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BPData *BpoolData = (BPData *)bm->mgmtData;
    BufferPoolFrame *currentframe = BpoolData->headframe;
    BufferPoolFrame *endframe = BpoolData->endframe;
  
    while (currentframe != NULL && currentframe->indexpage != pageNum)
    {
        currentframe = currentframe->nextframe;
    }

    if (currentframe != NULL && currentframe != endframe)
    {
        BufferPoolFrame *prevframe = currentframe->prevframe;
        BufferPoolFrame *nextframe = currentframe->nextframe;

        if (currentframe == BpoolData->headframe)
        {
            BpoolData->headframe = nextframe;
        }
        else
        {
            prevframe->nextframe = nextframe;
        }

        if (nextframe != NULL)
        {
            nextframe->prevframe = prevframe;
        }

        endframe->nextframe = currentframe;
        currentframe->prevframe = endframe;
        currentframe->nextframe = NULL;
        BpoolData->endframe = currentframe;
    }
}

