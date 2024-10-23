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

// Structure representing a node in the index tree
typedef struct IndexTree
{
    // Number of keys currently stored in this node
    int keyCount;
    // Maximum number of keys that can be stored in a node
    int maxKeysPerNode;
    // Total number of nodes in the tree
    int nodeCount;
    // Record identifier (page and slot) for the indexed record
    struct RID rid;
    // Value stored in the node (can be int, float, string, or bool)
    struct Value value;
    // Buffer pool for managing page access
    BM_BufferPool *bufferPool;
    // Handle for accessing page data
    BM_PageHandle *pageHandle;
} IndexTree;

// Global Variables

// Total number of keys across all nodes in the tree
int totalKeyCount;
// Current position during tree scanning operations
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
    // Nothing for intialization
    return RC_OK;
}

RC shutdownIndexManager()
{
    // freeing each element in indexTreeArray
    for (int i = 0; i < totalKeyCount; i++)
    {
        free(indexTreeArray[i]);
    }
    // at the end free whole indexTreeArray
    free(indexTreeArray);
    return RC_OK;
}

// Creates a new B-tree index file with specified parameters
RC createBtree(char *idxId, DataType keyType, int n)
{
    // Initialize file handling structures
    SM_FileHandle fileHandle;
    SM_PageHandle pageBuffer = malloc(PAGE_SIZE * sizeof(char));

    if (!pageBuffer)
        return RC_MEM_ALLOC_FAILURE;

    // Create and open the index file
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

    // Ensure the file has at least one page
    status = ensureCapacity(1, &fileHandle);
    if (status != RC_OK)
    {
        free(pageBuffer);
        closePageFile(&fileHandle);
        return status;
    }

    // Allocate memory for the index tree array
    indexTreeArray = malloc(sizeof(IndexTree *) * MAX_KEYS);
    if (!indexTreeArray)
    {
        free(pageBuffer);
        closePageFile(&fileHandle);
        return RC_MEM_ALLOC_FAILURE;
    }

    // Allocate memory for each tree node
    for (int treeIndex = 0; treeIndex < MAX_KEYS; treeIndex++)
    {
        indexTreeArray[treeIndex] = malloc(sizeof(IndexTree));
        if (!indexTreeArray[treeIndex])
        {
            // Clean up on allocation failure
            for (int freeIndex = 0; freeIndex < treeIndex; freeIndex++)
                free(indexTreeArray[freeIndex]);
            free(indexTreeArray);
            free(pageBuffer);
            closePageFile(&fileHandle);
            return RC_MEM_ALLOC_FAILURE;
        }
    }

    // Write the maximum keys per node to the first page
    *((int *)pageBuffer) = n;
    status = writeCurrentBlock(&fileHandle, pageBuffer);
    if (status != RC_OK)
    {
        // Clean up on write failure
        for (int freeIndex = 0; freeIndex < MAX_KEYS; freeIndex++)
            free(indexTreeArray[freeIndex]);
        free(indexTreeArray);
        free(pageBuffer);
        closePageFile(&fileHandle);
        return status;
    }

    // Close the file and initialize counters
    status = closePageFile(&fileHandle);
    if (status != RC_OK)
    {
        // Clean up on close failure
        for (int freeIndex = 0; freeIndex < MAX_KEYS; freeIndex++)
            free(indexTreeArray[freeIndex]);
        free(indexTreeArray);
        free(pageBuffer);
        return status;
    }

    // Initialize global counters
    totalKeyCount = 0;
    currentScanPosition = 0;
    free(pageBuffer);

    return RC_OK;
}

// Opens an existing B-tree index file
RC openBtree(BTreeHandle **tree, char *idxId)
{
    // Open the index file
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

    // Copy the index ID
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

    // Pin the first page to read max keys per node
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

    // Unpin the page and close file
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

// Closes the B-tree and frees all associated memory
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

// Deletes the B-tree file and frees associated memory
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

// Counts unique nodes in the tree by checking for duplicate pages
RC getNumNodes(BTreeHandle *tree, int *result)
{
    if (!tree || !result)
        return RC_NULL_PARAM;

    int duplicatePageCount = 0;
    // Count duplicate pages
    for (int currentNode = 1; currentNode < totalKeyCount; currentNode++)
    {
        for (int previousNode = currentNode - 1; previousNode >= 0; previousNode--)
        {
            if (indexTreeArray[currentNode]->rid.page == indexTreeArray[previousNode]->rid.page)
            {
                duplicatePageCount++;
            }
        }
    }
    *result = totalKeyCount - duplicatePageCount;
    return RC_OK;
}

// Returns the total number of entries in the tree
RC getNumEntries(BTreeHandle *tree, int *result)
{
    if (!tree || !result)
        return RC_NULL_PARAM;
    *result = totalKeyCount;
    return RC_OK;
}

// Returns the data type of keys stored in the tree
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
    IndexTree *treeData = (IndexTree *)(tree->mgmtData);
    indexTreeArray[totalKeyCount] = malloc(sizeof(IndexTree));
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
    // Pin the page for writing
    RC status = pinPage(treeData->bufferPool, treeData->pageHandle, totalKeyCount);
    if (status != RC_OK)
        return status;

    // Mark page as modified and not full
    markDirty(treeData->bufferPool, treeData->pageHandle);
    treeData->pageHandle->data = "NotFull";

    // Set key value and RID
    status = setKeyValue(indexTreeArray[totalKeyCount], key);
    if (status != RC_OK)
    {
        unpinPage(treeData->bufferPool, treeData->pageHandle);
        return status;
    }

    // Update tree data
    indexTreeArray[totalKeyCount]->rid = rid;
    totalKeyCount++;

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
    // Handle different data types
    switch (key->dt)
    {
    case DT_INT:
        node->value.v.intV = key->v.intV;
        break;
    case DT_FLOAT:
        node->value.v.floatV = key->v.floatV;
        break;
    case DT_STRING:
        node->value.v.stringV = strdup(key->v.stringV);
        if (!node->value.v.stringV)
            return RC_MEM_ALLOC_FAILURE;
        break;
    case DT_BOOL:
        node->value.v.boolV = key->v.boolV;
        break;
    default:
        return RC_RM_NO_PRINT_FOR_DATATYPE;
    }
    return RC_OK;
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
    // Pin the page containing the key
    RC status = pinPage(treeData->bufferPool, treeData->pageHandle, (keyIndex / treeData->maxKeysPerNode));
    if (status != RC_OK)
        return status;

    // Update page status and unpin
    treeData->pageHandle->data = "NOT_FULL";
    markDirty(treeData->bufferPool, treeData->pageHandle);
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
    Value tempValue = indexTreeArray[firstIndex]->value;
    RID tempRid = indexTreeArray[firstIndex]->rid;
    indexTreeArray[firstIndex]->value = indexTreeArray[secondIndex]->value;
    indexTreeArray[firstIndex]->rid = indexTreeArray[secondIndex]->rid;
    indexTreeArray[secondIndex]->value = tempValue;
    indexTreeArray[secondIndex]->rid = tempRid;
}

// Returns the next entry in the tree scan
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    if (!handle || !result)
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
        if (index % compareValue == 0 && index != 0)
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