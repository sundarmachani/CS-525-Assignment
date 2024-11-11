#include "buffer_mgr.h"
#include "stdlib.h"
#include "record_mgr.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "string.h"
#include <stdarg.h>

#define MAX_STRING_LENGTH 1000
#define MAX_KEYS 100
#define NUM_OF_PAGES 10

// Struct for node in Index Tree
typedef struct IndexTree
{
    // No. of keys stored in this node
    int keyCount;
    // Max no. of keys that can be stored in a node
    int maxKeysPerNode;
    // Total no. of nodes in the tree
    int nodeCount;
    // Record identifier, slot and page, the indexed record
    struct RID rid;
    // Value stored in  node, can be either int, float, string, or bool
    struct Value value;
    // Buffer pool - page access
    BM_BufferPool *bufferPool;
    // Handle to access page data
    BM_PageHandle *pageHandle;
} IndexTree;


// Total keys throughout all nodes in the tree
int totalKeyCount;
// Current position during tree scanning 
int currentScanPosition;
// Array of pointers to IndexTree nodes
IndexTree **indexTreeArray;

// helper functions
int compareKeys(Value *key1, Value *key2);
static RC setKeyValue(IndexTree *node, Value *key);
static RC insertNewKey(IndexTree *treeInfo, Value *key, RID rid);
static bool keyExists(Value *key);
static RC insertFirstKey(IndexTree *treeInfo, Value *key, RID rid);
static RC updatePageStatus(IndexTree *treeInfo, int index);
static void shiftKeysAfterDeletion(int startIndex);
static RC sortKeys();
static int findSmallestKeyIndex(int startIndex, DataType keyType);
static int compareKeysForSort(int index1, int index2, DataType keyType);
static void swapKeys(int index1, int index2);
static void appendToString(char *dest, const char *format, ...);
static void removeTrailingComma(char *str);

RC initIndexManager(void *mgmtData)
{
    // nothing for init
    return RC_OK;
}

RC shutdownIndexManager()
{
    // free each element in indexTreeArray
    for (int i = 0; i < totalKeyCount; i++)
    {
        free(indexTreeArray[i]);
    }
    // free whole IndexTreeArray at the end
    free(indexTreeArray);
    return RC_OK;
}

// Creates a new B-tree index file
RC createBtree(char *idxId, DataType keyType, int n)
{
    // Initialize file handling structs
    SM_FileHandle fileHandle;
    size_t chSize = sizeof(char);
    SM_PageHandle pageBuffer = malloc(PAGE_SIZE * chSize);

    if (!pageBuffer)
        return RC_MEM_ALLOC_FAILURE;

    // Create, open the index file
    RC status = createPageFile(idxId);
    if (status != RC_OK)
    {
        free(pageBuffer);
        return status;
    }

    status = openPageFile(idxId, &fileHandle);
    if (status != RC_OK)
    {
        free(pageBuffer);
        return status;
    }

    // Ensure file has at least 1 page
    status = ensureCapacity(1, &fileHandle);
    if (status != RC_OK)
    {
        free(pageBuffer);
        closePageFile(&fileHandle);
        return status;
    }

    // Allocate memory for  index tree array
    indexTreeArray = malloc(sizeof(IndexTree *) * MAX_KEYS);
    if (!indexTreeArray)
    {
        free(pageBuffer);
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Allocate memory for each node in the tree
    for (int treeIndex = 0; treeIndex < MAX_KEYS; treeIndex++)
    {
        indexTreeArray[treeIndex] = malloc(sizeof(IndexTree));
        if (!indexTreeArray[treeIndex])
        {
            // Clean up when allocation fails
            for (int freeIndex = 0; freeIndex < treeIndex; freeIndex++)
                free(indexTreeArray[freeIndex]);
            free(indexTreeArray);
            free(pageBuffer);
            closePageFile(&fileHandle);
            return RC_MEM_ALLOC_FAILURE;
        }
    }

    // Write max keys per node to the first page
    *((int *)pageBuffer) = n;
    status = writeCurrentBlock(&fileHandle, pageBuffer);
    if (status != RC_OK)
    {
        // Clean up when write fails
        for (int freeIndex = 0; freeIndex < MAX_KEYS; freeIndex++)
            free(indexTreeArray[freeIndex]);
        free(indexTreeArray);
        free(pageBuffer);
        closePageFile(&fileHandle);
        return status;
    }

    // Close file, init counters
    status = closePageFile(&fileHandle);
    if (status != RC_OK)
    {
        // Clean up when close fails
        for (int freeIndex = 0; freeIndex < MAX_KEYS; freeIndex++)
            free(indexTreeArray[freeIndex]);
        free(indexTreeArray);
        free(pageBuffer);
        return status;
    }

    // global counters 
    totalKeyCount = 0;
    currentScanPosition = 0;
    free(pageBuffer);

    return RC_OK;
}

// Opens existing B-tree index file
RC openBtree(BTreeHandle **tree, char *idxId)
{
    // Open index file
    SM_FileHandle fileHandle;
    RC status = openPageFile(idxId, &fileHandle);
    if (status != RC_OK)
        return status;

    // Allocate memory for tree handle
    *tree = malloc(sizeof(BTreeHandle));
    if (!*tree)
    {
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Copy index ID
    (*tree)->idxId = strdup(idxId);
    if (!(*tree)->idxId)
    {
        free(*tree);
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Initialize tree data structure
    IndexTree *treeData = malloc(sizeof(IndexTree));
    if (!treeData)
    {
        free((*tree)->idxId);
        free(*tree);
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Create buffer pool and page handle
    treeData->bufferPool = MAKE_POOL();
    treeData->pageHandle = MAKE_PAGE_HANDLE();
    if (!treeData->bufferPool || !treeData->pageHandle)
    {
        free(treeData);
        free((*tree)->idxId);
        free(*tree);
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Initialize buffer pool
    status = initBufferPool(treeData->bufferPool, idxId, NUM_OF_PAGES, RS_FIFO, NULL);
    if (status != RC_OK)
    {
        free(treeData->pageHandle);
        free(treeData->bufferPool);
        free(treeData);
        free((*tree)->idxId);
        free(*tree);
        closePageFile(&fileHandle);
        return status;
    }

    // Pin first page to read max keys per node
    status = pinPage(treeData->bufferPool, treeData->pageHandle, 1);
    if (status != RC_OK)
    {
        shutdownBufferPool(treeData->bufferPool);
        free(treeData->pageHandle);
        free(treeData->bufferPool);
        free(treeData);
        free((*tree)->idxId);
        free(*tree);
        closePageFile(&fileHandle);
        return status;
    }

    // Initialize tree data
    treeData->maxKeysPerNode = *((int *)treeData->pageHandle->data);
    treeData->nodeCount = 0;
    (*tree)->mgmtData = treeData;

    // Unpin page, close file
    status = unpinPage(treeData->bufferPool, treeData->pageHandle);
    if (status != RC_OK)
    {
        shutdownBufferPool(treeData->bufferPool);
        free(treeData->pageHandle);
        free(treeData->bufferPool);
        free(treeData);
        free((*tree)->idxId);
        free(*tree);
        closePageFile(&fileHandle);
        return status;
    }

    return closePageFile(&fileHandle);
}

// Closes B-tree, frees all associated memory
RC closeBtree(BTreeHandle *tree)
{
    if (tree)
    {
        IndexTree *treeData = (IndexTree *)tree->mgmtData;
        if (treeData)
        {
            // Clean up buffer pool
            if (treeData->bufferPool)
            {
                shutdownBufferPool(treeData->bufferPool);
                free(treeData->bufferPool);
            }
            // Free page handle and tree data
            free(treeData->pageHandle);
            free(treeData);
        }
        // Free tree handle resources
        free(tree->idxId);
        free(tree);
    }
    return RC_OK;
}

// Delete B-tree file and free memory
RC deleteBtree(char *idxId)
{
    RC status = destroyPageFile(idxId);
    if (status == RC_OK)
    {
        // Clean up index tree array
        if (indexTreeArray)
        {
            for (int index = 0; index < totalKeyCount; index++)
            {
                free(indexTreeArray[index]);
            }
            free(indexTreeArray);
            indexTreeArray = NULL;
        }
        // Reset counters
        totalKeyCount = 0;
        currentScanPosition = 0;
    }
    return status;
}

// Counts unique nodes in tree by checking for duplicate pages
RC getNumNodes(BTreeHandle *tree, int *result)
{
    if (!tree)
        return RC_NULL_PARAM;
    if (!result)
        return RC_NULL_PARAM;
        
    int duplicatePageCount = 0;
    for (int currentNode = 1; currentNode < totalKeyCount; currentNode++)
    {
        int previousNode = currentNode - 1;
        while (previousNode >= 0)
        {
            int currentPage = indexTreeArray[currentNode]->rid.page;
            int previousPage = indexTreeArray[previousNode]->rid.page;
            if (currentPage == previousPage)
            {
                duplicatePageCount++;
            }
            previousNode--;
        }
    }
    *result = totalKeyCount - duplicatePageCount;
    return RC_OK;
}



// Returns  total number of entries in the tree
RC getNumEntries(BTreeHandle *tree, int *result)
{
    if (!tree)
        return RC_NULL_PARAM;
    if (!result)
        return RC_NULL_PARAM;
        
    *result = totalKeyCount;
    return RC_OK;
}

// Returns data type of keys stored in the tree
RC getKeyType(BTreeHandle *tree, DataType *result)
{
    if (!tree || !result)
        return RC_NULL_PARAM;
    *result = tree->keyType;
    return RC_OK;
}

// Searches for a key in the tree and returns its RID if found
RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    for (int index = 0; index < totalKeyCount; index++)
    {
        // Check if data types match
        if (indexTreeArray[index]->value.dt == key->dt)
        {
            bool isKeyMatch = false;
            // Compare values based on data type
            switch (key->dt)
            {
            case DT_INT:
                isKeyMatch = (indexTreeArray[index]->value.v.intV == key->v.intV);
                break;
            case DT_FLOAT:
                isKeyMatch = (indexTreeArray[index]->value.v.floatV == key->v.floatV);
                break;
            case DT_STRING:
                isKeyMatch = (strcmp(indexTreeArray[index]->value.v.stringV, key->v.stringV) == 0);
                break;
            case DT_BOOL:
                isKeyMatch = (indexTreeArray[index]->value.v.boolV == key->v.boolV);
                break;
            default:
                return RC_RM_NO_PRINT_FOR_DATATYPE;
            }
            // Return RID if key is found
            if (isKeyMatch)
            {
                *result = indexTreeArray[index]->rid;
                return RC_OK;
            }
        }
    }
    return RC_IM_KEY_NOT_FOUND;
}

// Inserts a new key-RID pair into the B-tree
RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
    // Get tree management data
    size_t indTreesize = sizeof(IndexTree);
    IndexTree *treeData = (IndexTree *)(tree->mgmtData);
    indexTreeArray[totalKeyCount] = malloc(indTreesize);
    if (!indexTreeArray[totalKeyCount])
        return RC_MEM_ALLOC_FAILURE;

    // Handle first key insertion
    if (totalKeyCount == 0)
    {
        return insertFirstKey(treeData, key, rid);
    }

    // Check for duplicate keys
    if (keyExists(key))
    {
        free(indexTreeArray[totalKeyCount]);
        return RC_IM_KEY_ALREADY_EXISTS;
    }

    return insertNewKey(treeData, key, rid);
}

// Handles insertion of the first key in the tree
static RC insertFirstKey(IndexTree *treeData, Value *key, RID rid)
{
    char *PAGE_STATUS = "NotFull";
    RC status;
    
    // Pin and prepare page
    status = pinPage(treeData->bufferPool, treeData->pageHandle, totalKeyCount);
    if (status != RC_OK) {
        return status;
    }
    
    // Mark page as modified
    status = markDirty(treeData->bufferPool, treeData->pageHandle);
    if (status != RC_OK) {
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        return status;
    }
    treeData->pageHandle->data = PAGE_STATUS;
    
    // Insert new key
    IndexTree *newNode = indexTreeArray[totalKeyCount];
    status = setKeyValue(newNode, key);
    if (status != RC_OK) {
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        return status;
    }
    
    // Update node data
    newNode->rid = rid;
    totalKeyCount++;
    
    // Release page 
    return unpinPage(treeData->bufferPool, treeData->pageHandle);
}

// Checks if a key already exists in the tree
static bool keyExists(Value *key)
{
    for (int keyIndex = 0; keyIndex < totalKeyCount; keyIndex++)
    {
        if (compareKeys(&indexTreeArray[keyIndex]->value, key) == 0)
        {
            return true;
        }
    }
    return false;
}

// Inserts a new key into an existing tree
static RC insertNewKey(IndexTree *treeData, Value *key, RID rid)
{
    // Pin the current node's page
    RC status = pinPage(treeData->bufferPool, treeData->pageHandle, treeData->nodeCount);
    if (status != RC_OK)
        return status;

    markDirty(treeData->bufferPool, treeData->pageHandle);

    // Handle node overflow
    if (strcmp(treeData->pageHandle->data, "NodeFull") == 0)
    {
        // Create new node
        treeData->nodeCount++;
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        status = pinPage(treeData->bufferPool, treeData->pageHandle, treeData->nodeCount);
        if (status != RC_OK)
            return status;
        treeData->pageHandle->data = "NotFull";
    }
    else
    {
        treeData->pageHandle->data = "NodeFull";
    }

    // Set key value and update tree
    status = setKeyValue(indexTreeArray[totalKeyCount], key);
    if (status != RC_OK)
    {
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        return status;
    }

    indexTreeArray[totalKeyCount]->rid = rid;
    totalKeyCount++;

    return unpinPage(treeData->bufferPool, treeData->pageHandle);
}

// Sets the value of a key in a node based on its data type
static RC setKeyValue(IndexTree *node, Value *key)
{
    node->value.dt = key->dt;

    switch (key->dt)
    {
    case DT_INT:
        node->value.v.intV = key->v.intV;
        return RC_OK;
        
    case DT_FLOAT:
        node->value.v.floatV = key->v.floatV;
        return RC_OK;
        
    case DT_STRING:
        if (!(node->value.v.stringV = strdup(key->v.stringV))) {
            return RC_MEM_ALLOC_FAILURE;
        }
        return RC_OK;
        
    case DT_BOOL:
        node->value.v.boolV = key->v.boolV;
        return RC_OK;
        
    default:
        return RC_RM_NO_PRINT_FOR_DATATYPE;
    }
}

// Compares two keys of the same data type, returns -1 (less), 0 (equal), or 1 (greater)
int compareKeys(Value *key1, Value *key2)
{
    // Check if keys have same data type
    if (key1->dt != key2->dt)
    {
        return -2; // Different data types, cannot compare
    }

    // Compare based on data type
    switch (key1->dt)
    {
    case DT_INT:
        return (key1->v.intV == key2->v.intV) ? 0 : (key1->v.intV < key2->v.intV) ? -1
                                                                                  : 1;
    case DT_FLOAT:
        return (key1->v.floatV == key2->v.floatV) ? 0 : (key1->v.floatV < key2->v.floatV) ? -1
                                                                                          : 1;
    case DT_STRING:
        return strcmp(key1->v.stringV, key2->v.stringV);
    case DT_BOOL:
        return (key1->v.boolV == key2->v.boolV) ? 0 : (key1->v.boolV < key2->v.boolV) ? -1
                                                                                      : 1;
    default:
        return -2;
    }
}

// Deletes a key from the tree
RC deleteKey(BTreeHandle *tree, Value *key)
{
    IndexTree *treeData = (IndexTree *)(tree->mgmtData);

    // Search for key to delete
    for (int keyIndex = 0; keyIndex < totalKeyCount; keyIndex++)
    {
        if (compareKeys(&indexTreeArray[keyIndex]->value, key) == 0)
        {
            RC status = updatePageStatus(treeData, keyIndex);
            if (status != RC_OK)
                return status;

            shiftKeysAfterDeletion(keyIndex);
            totalKeyCount--;
            return RC_OK;
        }
    }

    return RC_IM_KEY_NOT_FOUND;
}

// Updates page status after key deletion
static RC updatePageStatus(IndexTree *treeData, int keyIndex)
{
    const int pageNum = keyIndex / treeData->maxKeysPerNode;
    char *PAGE_STATUS = "NOT_FULL";
    RC status;
    
    // Pin the target page
    status = pinPage(treeData->bufferPool, treeData->pageHandle, pageNum);
    if (status != RC_OK) {
        return status;
    }
    
    //  Update page content
    treeData->pageHandle->data = PAGE_STATUS;
    status = markDirty(treeData->bufferPool, treeData->pageHandle);
    if (status != RC_OK) {
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        return status;
    }
    
    // Release the page
    return unpinPage(treeData->bufferPool, treeData->pageHandle);
}

// Shifts keys to fill gap after deletion
static void shiftKeysAfterDeletion(int startIndex)
{
    for (int shiftIndex = startIndex; shiftIndex < totalKeyCount - 1; shiftIndex++)
    {
        indexTreeArray[shiftIndex]->value = indexTreeArray[shiftIndex + 1]->value;
        indexTreeArray[shiftIndex]->rid = indexTreeArray[shiftIndex + 1]->rid;
    }
}

// Initializes a scan handle for tree traversal
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    if (!tree || !handle)
        return RC_NULL_PARAM;
    if (totalKeyCount == 0)
        return RC_IM_NO_MORE_ENTRIES;

    // Initialize scan handle
    *handle = malloc(sizeof(BT_ScanHandle));
    if (!*handle)
        return RC_MEM_ALLOC_FAILURE;

    (*handle)->tree = tree;
    (*handle)->currentPosition = 0;

    return sortKeys();
}

// Sorts all keys in the tree
static RC sortKeys()
{
    if (totalKeyCount <= 1)
        return RC_OK;

    DataType keyType = indexTreeArray[0]->value.dt;

    // Selection sort implementation
    for (int currentIndex = 0; currentIndex < totalKeyCount - 1; currentIndex++)
    {
        int smallestKeyIndex = findSmallestKeyIndex(currentIndex, keyType);
        if (smallestKeyIndex != currentIndex)
        {
            swapKeys(currentIndex, smallestKeyIndex);
        }
    }

    return RC_OK;
}

// Finds index of smallest key in unsorted portion
static int findSmallestKeyIndex(int startIndex, DataType keyType)
{
    int smallestIndex = startIndex;
    for (int compareIndex = startIndex + 1; compareIndex < totalKeyCount; compareIndex++)
    {
        if (compareKeysForSort(compareIndex, smallestIndex, keyType) < 0)
        {
            smallestIndex = compareIndex;
        }
    }
    return smallestIndex;
}

// Compares two keys for sorting
static int compareKeysForSort(int firstIndex, int secondIndex, DataType keyType)
{
    switch (keyType)
    {
    case DT_INT:
        return indexTreeArray[firstIndex]->value.v.intV - indexTreeArray[secondIndex]->value.v.intV;
    case DT_FLOAT:
        return (indexTreeArray[firstIndex]->value.v.floatV < indexTreeArray[secondIndex]->value.v.floatV) ? -1 : (indexTreeArray[firstIndex]->value.v.floatV > indexTreeArray[secondIndex]->value.v.floatV) ? 1
                                                                                                                                                                                                            : 0;
    case DT_STRING:
        return strcmp(indexTreeArray[firstIndex]->value.v.stringV, indexTreeArray[secondIndex]->value.v.stringV);
    case DT_BOOL:
        return indexTreeArray[firstIndex]->value.v.boolV - indexTreeArray[secondIndex]->value.v.boolV;
    default:
        return 0;
    }
}

// Swaps two keys in the tree
static void swapKeys(int firstIndex, int secondIndex)
{
    struct IndexTree* first = indexTreeArray[firstIndex];
    struct IndexTree* second = indexTreeArray[secondIndex];
    
    Value tempValue = first->value;
    RID tempRid = first->rid;
    
    first->value = second->value;
    first->rid = second->rid;
    
    second->value = tempValue;
    second->rid = tempRid;
}

// Returns the next entry in the tree scan
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    if (!handle)
        return RC_NULL_PARAM;
    if (!result)
        return RC_NULL_PARAM;
    // Check if there are more entries to scan
    if (handle->currentPosition < totalKeyCount)
    {
        *result = indexTreeArray[handle->currentPosition]->rid;
        handle->currentPosition++;
        return RC_OK;
    }

    return RC_IM_NO_MORE_ENTRIES;
}

// Closes the tree scan and frees resources
RC closeTreeScan(BT_ScanHandle *handle)
{
    if (!handle)
        return RC_NULL_PARAM;

    // Reset position and free handle
    handle->currentPosition = 0;
    free(handle);
    return RC_OK;
}

// Utility function to append formatted string to destination
static void appendToString(char *destination, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    char buffer[MAX_STRING_LENGTH];
    // Format string with variable arguments
    vsnprintf(buffer, sizeof(buffer), format, args);
    strcat(destination, buffer);
    va_end(args);
}

// Removes trailing comma from a string
static void removeTrailingComma(char *str)
{
    size_t length = strlen(str);
    if (length > 0 && str[length - 1] == ',')
    {
        str[length - 1] = '\0';
    }
}

// Prints tree structure and returns tree ID
char *printTree(BTreeHandle *tree)
{
    if (!tree || !indexTreeArray)
        return NULL;

    // Initialize strings and counters
    char operationString[MAX_STRING_LENGTH] = "1,";
    char treeString[MAX_STRING_LENGTH] = "";
    int nodeCount = 1, tempNodeCount = 1;
    int compareValue = 2;

    // Process each key in the tree
    for (int index = 0; index < totalKeyCount; index++)
    {
        // Handle node boundaries
        if (index != 0 && index % compareValue == 0)
        {
            appendToString(operationString, "%d,%d,",
                           indexTreeArray[index]->value.v.intV,
                           tempNodeCount + 1);
            tempNodeCount++;
            nodeCount++;
        }

        // Format node information
        char resultString[MAX_STRING_LENGTH] = "";
        if (index % compareValue == 0 && index != 0)
        {
            appendToString(resultString, "%d\n", nodeCount);
        }

        // Add key and RID information
        appendToString(resultString, "%d.%d, %d,",
                       indexTreeArray[index]->rid.page,
                       indexTreeArray[index]->rid.slot,
                       indexTreeArray[index]->value.v.intV);

        // Append to tree string if not a node boundary
        if (!(index % compareValue == 0 && index != 0))
        {
            strcat(treeString, resultString);
        }
    }

    // Clean up strings and print
    removeTrailingComma(operationString);
    removeTrailingComma(treeString);
    printf("%s\n%s\n", operationString, treeString);

    return strdup(tree->idxId);
}