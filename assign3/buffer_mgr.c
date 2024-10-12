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
    struct BufferPoolFrame *nextFrame;
    int indexpage;
    struct BufferPoolFrame *prevFrame;
} BufferPoolFrame;

typedef struct BPData
{
    int pageframesavailable;
    int readoperations;
    int writeoperations;

    //  page metadata
    int *listPageNo;
    int *fixcounts;
    bool *dirtyflag;

    // linked list for BufferPoolFrame
    BufferPoolFrame *headFrame;
    BufferPoolFrame *currentFrame;
    BufferPoolFrame *endFrame;
    // page data
    char *BpoolData;

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
static PageNumber findPageInBuffer(BPData *bpData, PageNumber thepage, int numPages);
static BufferPoolFrame *firstframefind(BPData *bpData);
static void dirtypageneeded(BPData *bpData, SM_FileHandle *fileHandle);
static void updatenewpg(BPData *bpData, PageNumber pageNum);
static void reorder(BPData *bpData, BufferPoolFrame *temp);

/**
 * This Function initalizes the buffer pool, then allocates memory
 */
RC initBP(BPData *bpData, int numPages)
{
    bpData->dirtyflag = malloc(numPages * sizeof(bool));
    bpData->listPageNo = malloc(numPages * sizeof(int));
    bpData->BpoolData = calloc(numPages * PAGE_SIZE, sizeof(char));
    bpData->pageframesavailable = numPages;
    bpData->fixcounts = calloc(numPages, sizeof(int));

    if (!bpData->dirtyflag || !bpData->listPageNo || !bpData->BpoolData || !bpData->fixcounts)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    // Initialize metadata
    bpData->headFrame = NULL;
    bpData->currentFrame = NULL;
    bpData->endFrame = NULL;
    bpData->readoperations = 0;
    bpData->writeoperations = 0;

    for (int index = 0; index < numPages; index++)
    {
        bpData->listPageNo[index] = NO_PAGE;
        bpData->dirtyflag[index] = false;
    }

    return RC_OK;
}

/**
 * Initializes the buffer pool, then allocates memory. Checks if the page file name exists,
 * whether pool has initialized or not.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    // Check if the buffer pool structure and pageFileName are not NULL
    if (!bm || !pageFileName)
    {
        return RC_NULL_PARAM;
    }

    struct stat buffer;

    // Check if the file specified by pageFileName exists
    if (stat(pageFileName, &buffer) != 0)
    {
        return RC_FILE_NOT_FOUND;
    }

    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    // Allocate memory for the buffer pool data structure
    bm->mgmtData = calloc(1, sizeof(BPData));
    BPData *bm_bpData = (BPData *)bm->mgmtData;

    // Check if the allocation for the buffer pool data structure is successful
    if (!bm_bpData)
    {
        return RC_MEM_ALLOC_FAILURE;
    }
    if (initBP(bm_bpData, numPages) != RC_OK)
    {
        free(bm_bpData); // Clean up on failure
        return RC_MEM_ALLOC_FAILURE;
    }
    return RC_OK;
}

/**
 * Checks for pinned pages
 */
bool hasPinnedPages(const BPData *bpData, int numPages)
{
    for (int index = 0; index < numPages; index++)
    {
        if (bpData->fixcounts[index] > 0)
        {
            return true; // Found pinned page
        }
    }
    return false; // none found
}

/**
 * Frees all allocated memory for the buffer pool data.
 */
void freeBpData(BPData *bpData)
{
    free(bpData->listPageNo);
    free(bpData->fixcounts);
    free(bpData->BpoolData);
    free(bpData->dirtyflag);

    // Reset pointers to NULL after freeing
    bpData->listPageNo = NULL;
    bpData->BpoolData = NULL;
    bpData->dirtyflag = NULL;

    // Reset linked list pointers
    bpData->headFrame = NULL;
    bpData->currentFrame = NULL;
    bpData->endFrame = NULL;

    // Reset operation counters
    bpData->readoperations = 0;
    bpData->writeoperations = 0;
}

/**
 * Shut down buffer pool and flush dirty pages
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bm)
    {
        return RC_BUFFER_POOL_NOT_EXIST;
    }

    BPData *bpData = (BPData *)bm->mgmtData;
    if (!bpData)
    {
        return RC_BUFFER_POOL_DATA_NOT_EXIST;
    }

    if (hasPinnedPages(bpData, bm->numPages))
    {
        return RC_SHUTDOWN_POOL_ERROR;
    }

    forceFlushPool(bm);
    freeBpData(bpData);
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

    BPData *bpData = (BPData *)bm->mgmtData;

    // Iterate over each page in the buffer pool
    for (PageNumber pageNumber = 0; pageNumber < bm->numPages; pageNumber++)
    {
        // Check if the page is dirty (has been modified) and not fixed (pinned)
        if (bpData->dirtyflag[pageNumber] && bpData->fixcounts[pageNumber] == 0)
        {
            SM_PageHandle pageHandle = bpData->BpoolData + pageNumber * PAGE_SIZE * sizeof(char);
            int actualPageIndex = bpData->listPageNo[pageNumber];
            writeBlock(actualPageIndex, &fileHandle, pageHandle);
            bpData->dirtyflag[pageNumber] = false;
            bpData->writeoperations++;
        }
    }

    // Close page file
    return closePageFile(&fileHandle);
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
    BPData *bpData = (BPData *)bm->mgmtData;
    PageNumber tgtPage = page->pageNum;

    /* Find the page number in the buffer pool. */
    PageNumber bufferPoolPageNumber = findPageInBuffer(bpData, tgtPage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    bpData->dirtyflag[bufferPoolPageNumber] = true;
    return RC_OK;
}

/**
 * The function looks for a page number in the buffer pool, takes the buffer pool data structure, the targer page no.,
 * the total no. of pages, and returns the index of the pool where the page is found. or NO_PAGE if not found.
 */
static PageNumber findPageInBuffer(BPData *bpdta, PageNumber thepage, int numPages)
{
    BufferPoolFrame *thisPage = bpdta->headFrame;

    // Calculate the number of filled slots in the buffer pool
    int fullslots = numPages - bpdta->pageframesavailable;

    // Traverse the linked list
    for (int i = 0; i < fullslots && thisPage != NULL; i++)
    {
        if (thisPage->indexpage == thepage)
        {
            return thisPage->indexpool;
        }
        thisPage = thisPage->nextFrame;
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

    PageNumber tgtPage = page->pageNum;
    BPData *bpData = (BPData *)bm->mgmtData;

    PageNumber bufferPoolPageNumber = findPageInBuffer(bpData, tgtPage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    if (bpData->fixcounts[bufferPoolPageNumber] > 0)
    {
        bpData->fixcounts[bufferPoolPageNumber]--;
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
    char *mutablePageFile = strdup(pageFile);
    if (mutablePageFile == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }
    
    RC status = openPageFile(mutablePageFile, &fileHandle);
    free(mutablePageFile);

    if (status != RC_OK)
    {
        return status;
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
    BPData *bpData = (BPData *)bm->mgmtData;
    // Get the actual page number from the page handle
    PageNumber actualPageNumber = page->pageNum;
    // Find the index of the page in the buffer pool
    PageNumber bufferPoolPageNumber = findPageInBuffer(bpData, actualPageNumber, bm->numPages);

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
    bpData->dirtyflag[bufferPoolPageNumber] = false;
    // Increment write operations counter
    bpData->writeoperations++;
    return RC_OK;
}

// Helper function to get buffer pool data
static inline BPData *getBpData(BM_BufferPool *const bm)
{
    return (BPData *)bm->mgmtData;
}

/**
 * Returns an array of PageNumber containing the page numbers of all the frames in the buffer pool.
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    return getBpData(bm)->listPageNo;
}

/**
 * Returns an array of of bools that indicate whether each page in the buffer pool is dirty or not.
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    return getBpData(bm)->dirtyflag;
}

/**
 * Returns an array of integers showing the number of times each page in the buffer pool
 * has been fixed
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    return getBpData(bm)->fixcounts;
}

/**
 * Returns the number of read I/O operations performed by the buffer pool.
 */
int getNumReadIO(BM_BufferPool *const bm)
{
    return getBpData(bm)->readoperations;
}

/**
 * Returns the number of write I/O operations performed by the buffer pool.
 * A write operation is performed whenever a page is written to disk.
 */
int getNumWriteIO(BM_BufferPool *const bm)
{
    return getBpData(bm)->writeoperations;
}

/**
 * Finds the page number of a page in the cache.
 */
static bool findPageInCache(BPData *bpd, PageNumber pageNum, PageNumber *pgindexbp)
{
    for (BufferPoolFrame *temp = bpd->headFrame; temp != NULL; temp = temp->nextFrame)
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
static void addNewFrameToCache(BPData *bpData, PageNumber pageNum, PageNumber pgIndexBP)
{
    BufferPoolFrame *handle = malloc(sizeof(BufferPoolFrame));
    handle->indexpool = pgIndexBP;
    handle->indexpage = pageNum;
    handle->nextFrame = NULL;

    if (bpData->headFrame == NULL)
    {
        bpData->headFrame = bpData->endFrame = handle;
        handle->prevFrame = NULL;
    }
    else
    {
        bpData->endFrame->nextFrame = handle;
        handle->prevFrame = bpData->endFrame;
        bpData->endFrame = handle;
    }

    bpData->listPageNo[pgIndexBP] = pageNum;
    bpData->pageframesavailable--;
}

/**
 * This function handles a page that is already in the cache. It changes the page's fix count
 * and points the page handle to the correct page in the cache.
 */
void handleCachedPage(BM_BufferPool *const bm, BM_PageHandle *const page, PageNumber pgIndexBP, const PageNumber pageNum, BPData *bpData)
{
    if (bm->strategy == RS_LRU)
    {
        LRUCachePinPage(bm, page, pageNum); // Use pageNum here as well
    }
    page->data = bpData->BpoolData + pgIndexBP * PAGE_SIZE * sizeof(char);
    bpData->fixcounts[pgIndexBP] = 1;
}

/**
 * This function determines which page in the buffer pool should be replaced when a new page is requested.
 * It currently only supports the FIFO and LRU strategies.
 */
PageNumber selectPageReplacementFrame(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum, SM_FileHandle *sm_fileHandle)
{
    if (bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
    {
        return LRUpinPageFIFO(bm, page, pageNum, sm_fileHandle);
    }
    else
    {
        printf("\n \n \t \t \t \t \t \t Other Page Replacement Strategies are not available\n");
        return NO_PAGE; // Return a default value if no strategy is available
    }
}

/**
 * This function gets a page from the buffer pool for the given page number. If the page is already there,
 * it brings it to front. If not, it selects a new frame and that page is read from disk to the frame.
 * The frame is then added to the cache.
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Check for null pointers
    if (bm == NULL || page == NULL)
    {
        return RC_NULL_PARAM; // Define this error code as needed
    }

    // Open page file
    SM_FileHandle sm_fileHandle;
    RC status = openPageFile(bm->pageFile, &sm_fileHandle);
    if (status != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Ensure the page file has enough space for the requested page number
    if (sm_fileHandle.totalNumPages <= pageNum)
    {
        ensureCapacity(pageNum + 1, &sm_fileHandle);
    }

    page->pageNum = pageNum;
    BPData *bpData = (BPData *)bm->mgmtData;
    PageNumber pgIndexBP = NO_PAGE;

    // Check if the page is in the cache
    bool isPageInCache = findPageInCache(bpData, pageNum, &pgIndexBP);

    if (isPageInCache)
    {
        handleCachedPage(bm, page, pgIndexBP, pageNum, bpData);
    }
    else
    {
        // If not in cache, try to add it to the buffer pool
        if (bpData->pageframesavailable > 0)
        {
            pgIndexBP = bm->numPages - bpData->pageframesavailable; // Adjust based on your structure
            addNewFrameToCache(bpData, pageNum, pgIndexBP);
        }
        else
        {
            // Select a frame based on buffer pool strategy
            pgIndexBP = selectPageReplacementFrame(bm, page, pageNum, &sm_fileHandle);
        }

        // Ensure pgIndexBP is valid
        // Check against available frames in bpData
        if (pgIndexBP < 0 || pgIndexBP >= (bm->numPages - bpData->pageframesavailable))
        {
            return RC_PAGE_NOT_FOUND_IN_CACHE; // Handle this error appropriately
        }

        // Read the page from disk
        if (readBlock(page->pageNum, &sm_fileHandle, bpData->BpoolData + pgIndexBP * PAGE_SIZE * sizeof(char)) != RC_OK)
        {
            return RC_FILE_NOT_FOUND; // Handle read failure appropriately
        }
        bpData->readoperations++;
    }

    // Set page data and fix count
    page->data = bpData->BpoolData + pgIndexBP * PAGE_SIZE * sizeof(char);
    bpData->fixcounts[pgIndexBP] = 1;

    closePageFile(&sm_fileHandle);
    return RC_OK;
}

/*
 Here we get a page from the buffer pool for the page number and if it's there we bring it to the front.
 Else, we get a new frame, where the page is read from disk to the frame. We then add it to cache.
 */
PageNumber LRUpinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,
                          const PageNumber pageNum, SM_FileHandle *fileHandle)
{
    BPData *bpData = (BPData *)bm->mgmtData;
    PageNumber bufferPoolPageIndex = NO_PAGE;

    BufferPoolFrame *temp = firstframefind(bpData);

    if (temp)
    {
        reorder(bpData, temp);
        dirtypageneeded(bpData, fileHandle);
        updatenewpg(bpData, pageNum);

        bufferPoolPageIndex = bpData->endFrame->indexpool;
    }

    return bufferPoolPageIndex;
}

/*
Find the first frame in the buffer pool that's unpinned.
If none, return NULL.
 */
static BufferPoolFrame *firstframefind(BPData *bpData)
{
    BufferPoolFrame *temp = bpData->headFrame;
    while (temp)
    {
        if (bpData->fixcounts[temp->indexpool] == 0)
        {
            return temp;
        }
        temp = temp->nextFrame;
    }
    return NULL;
}

/*
 Reorders the frames' linked list. This function takes a frame found as an arg
 and puts it at the end of the list
 */
static void reorder(BPData *bpData, BufferPoolFrame *temp)
{
    // if the frame is not already at the end of the list
    if (temp != bpData->endFrame)
    {
        // move  frame to end of list
        bpData->endFrame->nextFrame = temp;

        temp->nextFrame->prevFrame = temp->prevFrame;

        if (temp == bpData->headFrame)
        {
            bpData->headFrame = bpData->headFrame->nextFrame;
        }
        else
        {
            // update next one
            temp->prevFrame->nextFrame = temp->nextFrame;
        }

        // update previous, next pointers
        temp->prevFrame = bpData->endFrame;
        bpData->endFrame = bpData->endFrame->nextFrame;
        bpData->endFrame->nextFrame = NULL;
    }
}

/*
 write dirty page to disk and mark as clean
 */
static void dirtypageneeded(BPData *bpData, SM_FileHandle *fileHandle)
{
    if (bpData->dirtyflag[bpData->endFrame->indexpool] == true)
    {
        // Point to data for page
        char *memory = bpData->BpoolData +
                       bpData->endFrame->indexpool * PAGE_SIZE * sizeof(char);
        // dirty page no.
        int oldPgNum = bpData->endFrame->indexpage;
        // write to disk
        writeBlock(oldPgNum, fileHandle, memory);
        // Mark clean
        bpData->dirtyflag[bpData->endFrame->indexpool] = false;
        bpData->writeoperations++;
    }
}

/*
 Update end frame of linked list for new page no.
 */
static void updatenewpg(BPData *bpData, PageNumber pageNum)
{
    bpData->endFrame->indexpage = pageNum;
    bpData->listPageNo[bpData->endFrame->indexpool] = pageNum;
}

void LRUCachePinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BPData *bpData = (BPData *)bm->mgmtData;
    BufferPoolFrame *currentFrame = bpData->headFrame;
    BufferPoolFrame *endFrame = bpData->endFrame;

    // Find frame w/ page no.
    while (currentFrame != NULL && currentFrame->indexpage != pageNum)
    {
        currentFrame = currentFrame->nextFrame;
    }

    if (currentFrame != NULL && currentFrame != endFrame)
    {
        if (currentFrame == bpData->headFrame)
        {
            bpData->headFrame = currentFrame->nextFrame;
        }
        else
        {
            currentFrame->prevFrame->nextFrame = currentFrame->nextFrame;
        }

        if (currentFrame->nextFrame != NULL)
        {
            currentFrame->nextFrame->prevFrame = currentFrame->prevFrame;
        }
        bpData->endFrame->nextFrame = currentFrame;
        currentFrame->prevFrame = bpData->endFrame;
        currentFrame->nextFrame = NULL;
        bpData->endFrame = currentFrame;
    }
}