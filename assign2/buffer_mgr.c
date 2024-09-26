#include "dt.h"
#include "buffer_mgr.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <limits.h>
#include "storage_mgr.h"

typedef struct BufferPoolFrame
{
    int poolIndex;
    int pageIndex;
    struct BufferPoolFrame *next;
    struct BufferPoolFrame *prev;
} BufferPoolFrame;

typedef struct BM_BufferPoolData
{
    int noOfAvailablePageFrames;
    int noOfReadOperations;
    int noOfWriteOperations;

    //  page metadata
    int *pageNumberList;
    int *fixCounts;
    bool *dirtyFlags;

    // page data
    char *poolData;

    // clock
    int *clockFlag;

    // linked list for BufferPoolFrame
    BufferPoolFrame *head, *current, *tail;
} BM_BufferPoolData;

// Struct to maintain page access history
typedef struct PageAccessHistory
{
    int *accessTimes; // Store the last K access times
    int count;        // Number of times the page has been accessed
} PageAccessHistory;

// Function prototypes
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle);
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
RC closePageFile(SM_FileHandle *fHandle);
void pinPageInCacheForLRU(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum);
int pinPageForFIFOLRU(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum, SM_FileHandle *fHandle);
// int pinPageForCLOCK(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum);
static PageNumber findPageInBuffer(BM_BufferPoolData *bufferPoolData, PageNumber targetPage, int numPages);

/**
 * Initializes the buffer pool data structure and allocates memory for all the
 * necessary data structures.
 */
RC initializeBufferPoolData(BM_BufferPoolData *bm_bufferPoolData, int numPages)
{
    bm_bufferPoolData->clockFlag = calloc(numPages * PAGE_SIZE, sizeof(int));
    bm_bufferPoolData->dirtyFlags = malloc(numPages * sizeof(bool));
    bm_bufferPoolData->pageNumberList = malloc(numPages * sizeof(int));
    bm_bufferPoolData->poolData = calloc(numPages * PAGE_SIZE, sizeof(char));
    bm_bufferPoolData->noOfAvailablePageFrames = numPages;
    bm_bufferPoolData->fixCounts = calloc(numPages, sizeof(int));

    if (!bm_bufferPoolData->clockFlag || !bm_bufferPoolData->dirtyFlags ||
        !bm_bufferPoolData->pageNumberList || !bm_bufferPoolData->poolData ||
        !bm_bufferPoolData->fixCounts)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    // Initialize metadata
    bm_bufferPoolData->head = NULL;
    bm_bufferPoolData->current = NULL;
    bm_bufferPoolData->tail = NULL;
    bm_bufferPoolData->noOfReadOperations = 0;
    bm_bufferPoolData->noOfWriteOperations = 0;

    for (int index = 0; index < numPages; index++)
    {
        bm_bufferPoolData->pageNumberList[index] = NO_PAGE;
        bm_bufferPoolData->dirtyFlags[index] = false;
    }

    return RC_OK;
}

/**
 * Initialize the buffer pool. This function initializes the buffer pool data
 * structure and allocates memory for all the necessary data structures. It
 * also checks if the file specified by pageFileName exists and if the buffer
 * pool initialization is successful, it returns RC_OK. Otherwise, it returns
 * appropriate error code.
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
    bm->mgmtData = calloc(1, sizeof(BM_BufferPoolData));
    BM_BufferPoolData *bm_bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;

    // Check if the allocation for the buffer pool data structure is successful
    if (!bm_bufferPoolData)
    {
        return RC_MEM_ALLOC_FAILURE;
    }
    if (initializeBufferPoolData(bm_bufferPoolData, numPages) != RC_OK)
    {
        free(bm_bufferPoolData); // Clean up on failure
        return RC_MEM_ALLOC_FAILURE;
    }
    return RC_OK;
}

/**
 * Checks if there are any pinned pages in the buffer pool.
 */
bool hasPinnedPages(const BM_BufferPoolData *bufferPoolData, int numPages) {
    for (int index = 0; index < numPages; index++) {
        if (bufferPoolData->fixCounts[index] > 0) {
            return true; // Found a pinned page
        }
    }
    return false; // No pinned pages found
}

/**
 * Frees all allocated memory for the buffer pool data.
 */
void freeBufferPoolData(BM_BufferPoolData *bufferPoolData) {
    free(bufferPoolData->pageNumberList);
    free(bufferPoolData->fixCounts);
    free(bufferPoolData->clockFlag);
    free(bufferPoolData->poolData);
    free(bufferPoolData->dirtyFlags);

    // Reset pointers to NULL after freeing
    bufferPoolData->pageNumberList = NULL;
    bufferPoolData->fixCounts = NULL;
    bufferPoolData->clockFlag = NULL;
    bufferPoolData->poolData = NULL;
    bufferPoolData->dirtyFlags = NULL;

    // Reset linked list pointers
    bufferPoolData->head = NULL;
    bufferPoolData->current = NULL;
    bufferPoolData->tail = NULL;

    // Reset operation counters
    bufferPoolData->noOfReadOperations = 0;
    bufferPoolData->noOfWriteOperations = 0;
}


/**
 * Shuts down the buffer pool. This function checks for pinned pages,
 * flushes dirty pages to disk, and frees allocated memory for buffer pool data.
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (!bm) {
        return RC_BUFFER_POOL_NOT_EXIST;
    }

    BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;
    if (!bufferPoolData) {
        return RC_BUFFER_POOL_DATA_NOT_EXIST;
    }

    if (hasPinnedPages(bufferPoolData, bm->numPages)) {
        return RC_SHUTDOWN_POOL_ERROR;
    }

    forceFlushPool(bm);
    freeBufferPoolData(bufferPoolData);
    free(bm->mgmtData);
    bm->mgmtData = NULL;

    return RC_OK;
}

/**
 * This function is used to force-flush the buffer pool, writing all dirty pages to disk.
 * It does this by iterating over each page in the buffer pool, checking if the page is dirty
 * and not fixed, and if so, writing the page to disk. It also resets the dirty flag for the page.
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
    SM_FileHandle fileHandle;
    int status = openPageFile(bm->pageFile, &fileHandle);

    if (status != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }

    BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;

    // Iterate over each page in the buffer pool
    for (PageNumber pageNumber = 0; pageNumber < bm->numPages; pageNumber++)
    {
        // Check if the page is dirty (has been modified) and not fixed (pinned)
        if (bufferPoolData->dirtyFlags[pageNumber] && bufferPoolData->fixCounts[pageNumber] == 0)
        {
            SM_PageHandle pageHandle = bufferPoolData->poolData + pageNumber * PAGE_SIZE * sizeof(char);
            int actualPageIndex = bufferPoolData->pageNumberList[pageNumber];
            writeBlock(actualPageIndex, &fileHandle, pageHandle);
            bufferPoolData->dirtyFlags[pageNumber] = false;
            bufferPoolData->noOfWriteOperations++;
        }
    }

    // Close the page file
    closePageFile(&fileHandle);
}

/**
 * This function marks a page in the buffer pool as dirty.
 * A dirty page is a page that has been modified since it was brought into the buffer pool.
 * It takes in the buffer pool structure, a pointer to the page handle struct, and the page number.
 * It returns a success code (RC_OK) if the page is found in the buffer pool and marked as dirty.
 * If the page is not found in the buffer pool, it returns an error code (RC_PAGE_NOT_FOUND_IN_CACHE).
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    /* Check if the buffer pool or page handle is NULL. If so, return an error code. */
    if (bm == NULL || page == NULL)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }
    BM_BufferPoolData *bpd = (BM_BufferPoolData *)bm->mgmtData;
    PageNumber targetPage = page->pageNum;

    /* Find the page number in the buffer pool. */
    PageNumber bufferPoolPageNumber = findPageInBuffer(bpd, targetPage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    bpd->dirtyFlags[bufferPoolPageNumber] = true;
    return RC_OK;
}

/**
 * This function searches for a specific page number in the buffer pool.
 * It takes in the buffer pool data structure, the target page number, and the total number of pages in the buffer pool.
 * It returns the index of the pool where the target page is found, or NO_PAGE if the page is not found in the buffer pool.
 */
static PageNumber findPageInBuffer(BM_BufferPoolData *bufferPoolData, PageNumber targetPage, int numPages)
{
    BufferPoolFrame *currentPage = bufferPoolData->head;

    // Calculate the number of filled slots in the buffer pool
    int filledSlots = numPages - bufferPoolData->noOfAvailablePageFrames;

    // Traverse the linked list until we've traversed all the filled slots or we've reached the end of the list
    for (int i = 0; i < filledSlots && currentPage != NULL; i++)
    {
        if (currentPage->pageIndex == targetPage)
        {
            return currentPage->poolIndex;
        }
        currentPage = currentPage->next;
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

    BM_BufferPoolData *bpd = (BM_BufferPoolData *)bm->mgmtData;
    PageNumber targetPage = page->pageNum;

    PageNumber bufferPoolPageNumber = findPageInBuffer(bpd, targetPage, bm->numPages);

    if (bufferPoolPageNumber == NO_PAGE)
    {
        return RC_PAGE_NOT_FOUND_IN_CACHE;
    }

    if (bpd->fixCounts[bufferPoolPageNumber] > 0)
    {
        bpd->fixCounts[bufferPoolPageNumber]--;
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
 * This function writes the page data to disk and marks the page as no longer dirty.
 * It first retrieves the buffer pool data and the actual page number from the page handle.
 * Then it uses the buffer pool data to find the index of the page in the buffer pool.
 * If the page is not found in the buffer pool, it returns an error.
 *
 * If the page is found, it writes the page data to disk using the page file and the actual page number.
 * It then resets the dirty flag for the page in the buffer pool data and increments the write operation counter.
 *
 * Finally, it returns a success code.
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Get the buffer pool data
    BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;
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

    // Mark the page as no longer dirty
    bufferPoolData->dirtyFlags[bufferPoolPageNumber] = false;
    // Increment the write operation counter
    bufferPoolData->noOfWriteOperations++;
    return RC_OK;
}

// Helper function to get buffer pool data
static inline BM_BufferPoolData *getBufferPoolData(BM_BufferPool *const bm)
{
    return (BM_BufferPoolData *)bm->mgmtData;
}

/**
 * Returns an array of PageNumber containing the page numbers of all the frames in the buffer pool.
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->pageNumberList;
}

/**
 * Returns an array of booleans indicating whether each page in the buffer pool is dirty or not.
 * A page is dirty if it has been modified since it was brought into the buffer pool.
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->dirtyFlags;
}

/**
 * Returns an array of integers indicating the number of times each page in the buffer pool
 * has been fixed (i.e. brought into the buffer pool).
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->fixCounts;
}

/**
 * Returns the number of read I/O operations performed by the buffer pool.
 */
int getNumReadIO(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->noOfReadOperations;
}

/**
 * Returns the number of write I/O operations performed by the buffer pool.
 * A write operation is performed whenever a page is written to disk.
 */
int getNumWriteIO(BM_BufferPool *const bm)
{
    return getBufferPoolData(bm)->noOfWriteOperations;
}

/**
 * Finds the page number of a page in the cache.
 */
static bool findPageInCache(BM_BufferPoolData *bpd, PageNumber pageNum, PageNumber *bufferPoolPageIndex)
{
    for (BufferPoolFrame *temp = bpd->head; temp != NULL; temp = temp->next)
    {
        if (temp->pageIndex == pageNum)
        {
            *bufferPoolPageIndex = temp->poolIndex;
            return true;
        }
    }
    return false;
}

/**
 * Adds a new frame to the cache.
 */
static void addNewFrameToCache(BM_BufferPoolData *bpd, PageNumber pageNum, PageNumber bufferPoolPageIndex)
{
    BufferPoolFrame *handle = malloc(sizeof(BufferPoolFrame));
    handle->poolIndex = bufferPoolPageIndex;
    handle->pageIndex = pageNum;
    handle->next = NULL;

    if (bpd->head == NULL)
    {
        bpd->head = bpd->tail = handle;
        handle->prev = NULL;
    }
    else
    {
        bpd->tail->next = handle;
        handle->prev = bpd->tail;
        bpd->tail = handle;
    }

    bpd->pageNumberList[bufferPoolPageIndex] = pageNum;
    bpd->noOfAvailablePageFrames--;
}

/**
 * Fetches a page from the buffer pool for the given page number. If the page is
 * already in the cache, it is brought to the front. If the page is not in the
 * cache, a new frame is selected from the buffer pool and the page is read from
 * disk into that frame. The frame is then added to the cache.
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Open the page file
    SM_FileHandle sm_fileHandle;
    RC status = openPageFile(bm->pageFile, &sm_fileHandle);
    if (status != RC_OK)
        return RC_FILE_NOT_FOUND;

    // If the page file is smaller than the requested page number, ensure that it
    // has enough space
    if (sm_fileHandle.totalNumPages <= pageNum)
    {
        ensureCapacity(pageNum + 1, &sm_fileHandle);
    }

    page->pageNum = pageNum;
    BM_BufferPoolData *bpd = (BM_BufferPoolData *)bm->mgmtData;
    PageNumber bufferPoolPageIndex = NO_PAGE;
    bool pageInCache = findPageInCache(bpd, pageNum, &bufferPoolPageIndex);

    // If the page is in the cache, bring it to the front and update the page handle
    if (pageInCache)
    {
        // If the buffer pool uses the LRU strategy, update the page's position in the cache
        if (bm->strategy == RS_LRU)
        {
            pinPageInCacheForLRU(bm, page, pageNum);
        }
        page->data = bpd->poolData + bufferPoolPageIndex * PAGE_SIZE * sizeof(char);
        bpd->fixCounts[bufferPoolPageIndex]++;
        bpd->clockFlag[bufferPoolPageIndex] = 1;
    }
    else
    {
        // If the page is not in the cache, try to find a frame to add it to
        if (bpd->noOfAvailablePageFrames > 0)
        {
            bufferPoolPageIndex = bm->numPages - bpd->noOfAvailablePageFrames;
            addNewFrameToCache(bpd, pageNum, bufferPoolPageIndex);
        }
        else
        {
            // If there is no space in the buffer pool, select a frame based on the buffer pool strategy
            if (bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
            {
                bufferPoolPageIndex = pinPageForFIFOLRU(bm, page, pageNum, &sm_fileHandle);
            }
            else if (bm->strategy == RS_LRU_K)
            {
                printf("Not implemented yet\n");
            }
            // else if (bm->strategy == RS_CLOCK)
            // {
            //     bufferPoolPageIndex = pinPageForCLOCK(bm, page, pageNum);
            // }
        }

        page->data = bpd->poolData + bufferPoolPageIndex * PAGE_SIZE * sizeof(char);
        bpd->fixCounts[bufferPoolPageIndex]++;
        bpd->clockFlag[bufferPoolPageIndex] = 1;

        // Only read from disk if the page is not in cache
        readBlock(page->pageNum, &sm_fileHandle, page->data);
        bpd->noOfReadOperations++;
    }

    closePageFile(&sm_fileHandle);
    return RC_OK;
}


PageNumber pinPageForFIFOLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum, SM_FileHandle *fileHandle)
{
    BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;
    BufferPoolFrame *temp = bufferPoolData->head;
    PageNumber bufferPoolPageIndex = NO_PAGE;

    while (temp != NULL)
    {
        if (bufferPoolData->fixCounts[temp->poolIndex] == 0)
        {
            break;
        }
        temp = temp->next;
    }
    if (temp != NULL)
    {
        if (temp != bufferPoolData->tail)
        {
            bufferPoolData->tail->next = temp;

            temp->next->prev = temp->prev;
            if (temp == bufferPoolData->head)
            {
                bufferPoolData->head = bufferPoolData->head->next;
            }
            else
            {
                temp->prev->next = temp->next;
            }
            temp->prev = bufferPoolData->tail;
            bufferPoolData->tail = bufferPoolData->tail->next;
            bufferPoolData->tail->next = NULL;
        }

        if (bufferPoolData->dirtyFlags[bufferPoolData->tail->poolIndex] == true)
        {
            char *memory = bufferPoolData->poolData + bufferPoolData->tail->poolIndex * PAGE_SIZE * sizeof(char);
            int old_pageNum = bufferPoolData->tail->pageIndex;
            writeBlock(old_pageNum, fileHandle, memory);
            bufferPoolData->dirtyFlags[bufferPoolData->tail->poolIndex] = false;
            bufferPoolData->noOfWriteOperations++;
        }
        bufferPoolData->tail->pageIndex = pageNum;
        bufferPoolData->pageNumberList[bufferPoolData->tail->poolIndex] = pageNum;
        bufferPoolPageIndex = bufferPoolData->tail->poolIndex;
    }

    return bufferPoolPageIndex;
}

void pinPageInCacheForLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;
    BufferPoolFrame *current = bufferPoolData->head;
    BufferPoolFrame *tail = bufferPoolData->tail;
    while (current != NULL)
    {
        if (current->pageIndex == pageNum)
        {
            break;
        }

        current = current->next;
    }

    if (current != tail)
    {
        bufferPoolData->tail->next = current;
        current->next->prev = current->prev;

        if (current == bufferPoolData->head)
        {
            bufferPoolData->head = bufferPoolData->head->next;
        }
        else
        {
            current->prev->next = current->next;
        }
        current->prev = bufferPoolData->tail;
        bufferPoolData->tail = bufferPoolData->tail->next;
        bufferPoolData->tail->next = NULL;
    }
}

// PageNumber pinPageForCLOCK(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
// {
//     BM_BufferPoolData *bufferPoolData = (BM_BufferPoolData *)bm->mgmtData;
//     BufferPoolFrame *temp = NULL;
//     PageNumber bufferPoolPageIndex = NO_PAGE;

//     if (bufferPoolData->current == NULL)
//     {
//         bufferPoolData->current = bufferPoolData->head;
//     }
//     while (bufferPoolData->clockFlag[bufferPoolData->current->poolIndex] == 1)
//     {
//         bufferPoolData->clockFlag[bufferPoolData->current->poolIndex] = 0;
//         if (bufferPoolData->current == bufferPoolData->tail)
//         {
//             bufferPoolData->current = bufferPoolData->head;
//         }
//         else
//         {
//             bufferPoolData->current = bufferPoolData->current->next;
//         }
//     }
//     bufferPoolData->current->pageIndex = pageNum;
//     bufferPoolData->pageNumberList[bufferPoolData->current->poolIndex] = pageNum;
//     bufferPoolData->clockFlag[bufferPoolData->current->poolIndex] = 1;
//     temp = bufferPoolData->current;

//     if (bufferPoolData->current == bufferPoolData->tail)
//     {
//         bufferPoolData->current = bufferPoolData->head;
//     }
//     else
//     {
//         bufferPoolData->current = bufferPoolData->current->next;
//     }
//     bufferPoolPageIndex = temp->poolIndex;

//     return bufferPoolPageIndex;
// }