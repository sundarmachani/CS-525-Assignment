#include <stdlib.h>
#include "dberror.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include "expr.h"

#define MAX_PAGE_FILE_NAME 255
#define SIZE_INT sizeof(int)
#define SIZE_FLOAT 15
#define SIZE_BOOL sizeof(bool)
#define DELIMITER_FIRST_ATTR '|'
#define DELIMITER_OTHER_ATTR ','
#define MAX_KEY_ATTRS 100

static char pageFile[MAX_PAGE_FILE_NAME];

static bool parseSchemaHeader(char *schemaCopy, char **token, char **context, Schema *schema);
static bool allocateSchemaMemory(Schema *schema);
static bool parseAttributes(char **token, char **context, Schema *schema);
static bool parseDataType(char *typeStr, DataType *dataType, int *typeLength);
static bool parseKeyAttributes(char **token, char **context, Schema *schema);

typedef struct ScanData
{
    /*page info */
    int thisPage;
    int numOfPages;

    /*slot info */
    int thisSlot;
    int totalNumSlots;

    Expr *theCondition;
} ScanData;

RC initRecordManager(void *mgmtData)
{
    // This function initializes the record manager
    // The parameter `mgmtData` is a placeholder for managing data that might be needed for initialization.
    // Here, the function returns RC_OK.
    return RC_OK;
}

/**
 * This function eleases any resources that the record manager
 *allocated. Nothing right now.
 */
RC shutdownRecordManager()
{
    return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
    RC rc;
    SM_FileHandle fileHandle;
    char *schemaToString = NULL;

    /* Check for invalid params */
    if (name == NULL || schema == NULL)
    {
        return RC_NULL_PARAM;
    }

    /* Copy  table name to a local variable.
     * This is done to ensure that the table name does not exceed the max limit.
     * limited to 255 chars. */
    strncpy(pageFile, name, MAX_PAGE_FILE_NAME);
    pageFile[MAX_PAGE_FILE_NAME - 1] = '\0';

    /* Create a new page file. The page file is empty to begin with */
    rc = createPageFile(name);
    if (rc != RC_OK)
    {
        return rc;
    }

    /* Open the page file. */
    rc = openPageFile(name, &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    /* Ensure that the page file has enough capacity to store schema  */
    rc = ensureCapacity(1, &fileHandle);
    if (rc != RC_OK)
    {
        closePageFile(&fileHandle);
        return rc;
    }

    /* Serialize  schema. */
    schemaToString = serializeSchema(schema);
    if (schemaToString == NULL)
    {
        closePageFile(&fileHandle);
        return RC_SERIALIZATION_ERROR;
    }

    /* Write the serialized schema to the page. */
    rc = writeBlock(0, &fileHandle, schemaToString);
    free(schemaToString);

    if (rc != RC_OK)
    {
        closePageFile(&fileHandle);
        return rc;
    }

    /* Close the page file . */
    rc = closePageFile(&fileHandle);
    return rc;
}

Schema *deserializeSchema(char *serializedSchema)
{
    //  Deserializes a schema from a given serialized schema string.
    if (serializedSchema == NULL || strlen(serializedSchema) == 0)
    {
        return NULL;
    }

    // Allocate memory for schema
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL;
    }

    // Duplicate serialized schema to parse.
    char *schemaCopy = strdup(serializedSchema);
    if (schemaCopy == NULL)
    {
        free(schema);
        return NULL;
    }

    // to parse schema:
    char *token;
    char *context;

    // Parse schema.
    if (!parseSchemaHeader(schemaCopy, &token, &context, schema) ||
        !allocateSchemaMemory(schema) ||
        !parseAttributes(&token, &context, schema) ||
        !parseKeyAttributes(&token, &context, schema))
    {
        // There was a problem parsing the schema, so free the schema and the copy.
        freeSchema(schema);
        free(schemaCopy);
        return NULL;
    }

    free(schemaCopy);
    return schema;
}

static bool parseSchemaHeader(char *schemaCopy, char **token, char **context, Schema *schema)
{
    // The schemaCopy is a string that looks like "<NUM_ATTRS>ATTR1:TYPE1,ATTR2:TYPE2,..."
    // We need to break it up into its component parts.
    // First, we use strtok_r to separate the schemaCopy into two parts: the part before the first '<'
    // and the part after the first '<'.  The part before the first '<' is ignored.
    // The part after the first '<' is the NUM_ATTRS, which we will convert to an integer.
    *token = strtok_r(schemaCopy, "<", context);
    if (*token == NULL)
    {
        return false;
    }

    // Next, we use strtok_r to separate the schemaCopy into two parts: the part before the first '>'
    // and the part after the first '>'.  The part before the first '>' is the NUM_ATTRS, which we just
    // parsed.  The part after the first '>' is the attributes, which we will parse later.
    *token = strtok_r(NULL, ">", context);
    if (*token == NULL)
    {
        return false;
    }

    // Finally, we convert the NUM_ATTRS from a string to an integer.
    schema->numAttr = atoi(*token);
    return (schema->numAttr > 0);
}

static bool allocateSchemaMemory(Schema *schema)
{
    // Allocate memory for attribute names, data types, and type lengths
    schema->attrNames = malloc(schema->numAttr * sizeof *schema->attrNames);
    schema->dataTypes = malloc(schema->numAttr * sizeof *schema->dataTypes);
    schema->typeLength = malloc(schema->numAttr * sizeof *schema->typeLength);

    // Return true if all allocations were successful, otherwise it is false
    return (schema->attrNames && schema->dataTypes && schema->typeLength);
}

static bool parseAttributes(char **token, char **context, Schema *schema)
{
    // Skip to start of attributes
    *token = strtok_r(NULL, "(", context);
    for (int i = 0; i < schema->numAttr; i++)
    {
        // Parse attr name
        *token = strtok_r(NULL, ": ", context);
        schema->attrNames[i] = strdup(*token);
        if (schema->attrNames[i] == NULL)
        {
            return false;
        }

        // Parse attr type
        *token = strtok_r(NULL, i == schema->numAttr - 1 ? ") " : ", ", context);
        if (!parseDataType(*token, &schema->dataTypes[i], &schema->typeLength[i]))
        {
            return false;
        }
    }
    return true;
}

static bool parseDataType(char *typeStr, DataType *dataType, int *typeLength)
{
    // Parsing of basic data types
    if (strcmp(typeStr, "INT") == 0)
    {
        *dataType = DT_INT;
        *typeLength = 0;
    }
    else if (strcmp(typeStr, "FLOAT") == 0)
    {
        *dataType = DT_FLOAT;
        *typeLength = 0;
    }
    else if (strcmp(typeStr, "BOOL") == 0)
    {
        *dataType = DT_BOOL;
        *typeLength = 0;
    }
    else
    {
        // Parse string type with length
        *dataType = DT_STRING;
        char *lengthStart = strchr(typeStr, '[');
        char *lengthEnd = strchr(typeStr, ']');
        if (lengthStart && lengthEnd)
        {
            *typeLength = atoi(lengthStart + 1);
        }
        else
        {
            return false;
        }
    }
    return true;
}

static bool parseKeyAttributes(char **token, char **context, Schema *schema)
{
    char *keyAttr[MAX_KEY_ATTRS];
    int keySize = 0;

    // Check for key attrs
    *token = strtok_r(NULL, "(", context);
    if (*token != NULL)
    {
        // Parse key attributes
        *token = strtok_r(NULL, ")", context);
        char *keyContext;
        char *keyToken = strtok_r(*token, ", ", &keyContext);

        // Store key attrs
        while (keyToken != NULL && keySize < MAX_KEY_ATTRS)
        {
            keyAttr[keySize] = strdup(keyToken);
            if (keyAttr[keySize] == NULL)
            {
                for (int k = 0; k < keySize; k++)
                {
                    free(keyAttr[k]);
                }
                return false;
            }
            keySize++;
            keyToken = strtok_r(NULL, ", ", &keyContext);
        }
    }

    if (keySize > 0)
    {
        // Allocate memory for key attrs
        schema->keyAttrs = (int *)malloc(sizeof(int) * keySize);
        if (schema->keyAttrs == NULL)
        {
            for (int k = 0; k < keySize; k++)
            {
                free(keyAttr[k]);
            }
            return false;
        }

        schema->keySize = keySize;

        // Map key attributes to indices
        for (int j = 0; j < keySize; j++)
        {
            for (int y = 0; y < schema->numAttr; y++)
            {
                if (strcmp(schema->attrNames[y], keyAttr[j]) == 0)
                {
                    schema->keyAttrs[j] = y;
                    break;
                }
            }
            free(keyAttr[j]);
        }
    }
    else
    {
        // No key attrs
        schema->keyAttrs = NULL;
        schema->keySize = 0;
    }

    return true;
}

RC openTable(RM_TableData *rel, char *name)
{
    // Check for null params
    if (!rel)
        return RC_NULL_PARAM;

    if (!name)
    {
        return RC_NULL_PARAM;
    }

    // Allocate memory for buffer pool and page handle
    BM_BufferPool *bm = malloc(sizeof(BM_BufferPool));
    BM_PageHandle *pageHandle = malloc(sizeof(BM_PageHandle));
    if (!bm || !pageHandle)
        return RC_MEM_ALLOC_FAILURE;

    RC rc;
    // Init the buffer pool
    rc = initBufferPool(bm, name, 3, RS_FIFO, NULL);
    if (rc != RC_OK)
        goto cleanup;

    // Pin  first page
    rc = pinPage(bm, pageHandle, 0);
    if (rc != RC_OK)
        goto cleanup;

    // Deserialize schema from first page
    Schema *deserializedSchema = deserializeSchema(pageHandle->data);
    unpinPage(bm, pageHandle);
    if (!deserializedSchema)
    {
        rc = RC_SCHEMA_DESERIALIZATION_ERRROR;
        goto cleanup;
    }

    // Set up RM_TableData structure
    rel->name = name;
    rel->mgmtData = bm;
    rel->schema = deserializedSchema;

cleanup:
    // Clean up on error
    if (rc != RC_OK)
    {
        if (bm)
        {
            shutdownBufferPool(bm);
        }
        if (pageHandle)
        {
            free(pageHandle);
        }
    }
    return rc;
}

RC closeTable(RM_TableData *rel)
{
    if (!rel)
        return RC_NULL_PARAM;

    Schema *schema = rel->schema;
    if (schema)
    {
        // Free attribute names
        char **names = schema->attrNames;
        if (names)
        {
            for (int i = 0; i < schema->numAttr; i++)
            {
                free(names[i]);
            }
            free(names);
        }

        // Free other schema components
        free(schema->typeLength);
        free(schema->dataTypes);
        free(schema->keyAttrs);
    }

    // Shutdown buffer pool and free memory
    shutdownBufferPool(rel->mgmtData);
    free(rel->mgmtData);
    free(schema);

    return RC_OK;
}

RC deleteTable(char *name)
{
    if (!name)
        return RC_NULL_PARAM;
    // Attempt to remove the file
    if (remove(name) != 0)
    {
        return RC_RM_TABLE_NOT_FOUND;
    }
    return RC_OK;
}

int getNumTuples(RM_TableData *rel)
{
    if (!rel)
        return -1;

    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle pageHandle;
    SM_FileHandle fileHandle;

    // Open the page file
    if (openPageFile(pageFile, &fileHandle) != RC_OK)
    {
        return -1;
    }

    int blockNum = 1, totalRecord = 0;
    // Iterate through all pages
    while (blockNum < fileHandle.totalNumPages)
    {
        // Pin each page
        if (pinPage(bm, &pageHandle, blockNum) != RC_OK)
        {
            closePageFile(&fileHandle);
            return -1;
        }
        // Count records in the page
        for (int i = 0; i < PAGE_SIZE; i++)
        {
            if (pageHandle.data[i] == '|')
                totalRecord++;
        }
        unpinPage(bm, &pageHandle);
        blockNum++;
    }
    closePageFile(&fileHandle);
    return totalRecord;
}

int getUsedPageSpace(char *pageData, Schema *schema)
{
    int spaceused = 0;
    int offset = 0;
    int recsize = getRecordSize(schema);

    // Iterate through the page data and calculate used space
    while (offset < PAGE_SIZE)
    {
        // Check for valid record
        if (pageData[offset] != '\0')
        {
            spaceused += recsize;
        }
        else
        {
            break; // Stop when reaching unused space
        }

        // Move to next record location
        offset += recsize;
    }

    return spaceused;
}

RC insertRecord(RM_TableData *rel, Record *record)
{
    // Initialize variables
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle pageHandle;
    SM_FileHandle fileHandle;
    PageNumber NoofPage = 1;
    int calcSlot = 0;
    int pgLen = 0;

    // Open the page file
    if (openPageFile(pageFile, &fileHandle) != RC_OK)
    {
        return RC_FILE_NOT_FOUND;
    }
    int NumberPagetotal = fileHandle.totalNumPages;
    closePageFile(&fileHandle);

    int recsize = getRecordSize(rel->schema);

    // Find space for the new record
    while (NoofPage <= NumberPagetotal)
    {
        pinPage(bm, &pageHandle, NoofPage);
        pgLen = getUsedPageSpace(pageHandle.data, rel->schema);
        int spaceleft = PAGE_SIZE - pgLen;

        if (recsize <= spaceleft)
        {
            calcSlot = pgLen / recsize;
            unpinPage(bm, &pageHandle);
            break;
        }

        unpinPage(bm, &pageHandle);
        NoofPage++;
    }

    // Insert the record
    pinPage(bm, &pageHandle, NoofPage);
    char *dtptr = pageHandle.data;
    pgLen = getUsedPageSpace(dtptr, rel->schema);
    char *recPtr = dtptr + pgLen;
    strcpy(recPtr, record->data);
    markDirty(bm, &pageHandle);
    unpinPage(bm, &pageHandle);

    record->id = (RID){.page = NoofPage, .slot = calcSlot};
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    // Check for null params
    if (rel == NULL)
    {
        return RC_NULL_PARAM;
    }

    // Initialize variables
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if (pageHandle == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    PageNumber pageNum = id.page;
    int numberofslot = id.slot;
    int recsize = getRecordSize(rel->schema) + 3;

    // Pin the page
    RC pinRC = pinPage(bm, pageHandle, pageNum);
    if (pinRC != RC_OK)
    {
        free(pageHandle);
        return pinRC;
    }

    // Mark the record as deleted
    char *dtptr = pageHandle->data;
    char *recPtr = dtptr + (recsize * numberofslot);
    *recPtr = '\0';

    // Mark the page as dirty and unpin
    RC markDirtyRC = markDirty(bm, pageHandle);
    if (markDirtyRC != RC_OK)
    {
        unpinPage(bm, pageHandle);
        free(pageHandle);
        return markDirtyRC;
    }

    RC unpinRC = unpinPage(bm, pageHandle);
    free(pageHandle);

    return unpinRC;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    // Check for null params
    if (rel == NULL)
    {
        return RC_NULL_PARAM;
    }

    if (record == NULL)
    {
        return RC_NULL_PARAM;
    }

    // Initialize variables
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle pageHandle;
    PageNumber pageNum = record->id.page;
    int slotNum = record->id.slot;
    int recsize = getRecordSize(rel->schema);

    // Pin the page
    RC rc = pinPage(bm, &pageHandle, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }

    // Update the record
    char *dataPointer = pageHandle.data + slotNum * recsize;
    strncpy(dataPointer, record->data, recsize);
    markDirty(bm, &pageHandle);
    unpinPage(bm, &pageHandle);
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    // Check for null params
    if (rel == NULL)
    {
        return RC_NULL_PARAM;
    }

    if (record == NULL)
    {
        return RC_NULL_PARAM;
    }

    // Initialize variables
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if (pageHandle == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    PageNumber pageNum = id.page;
    int numberofSlot = id.slot;
    int recSize = getRecordSize(rel->schema);

    // Pin the page
    RC pinRC = pinPage(bm, pageHandle, pageNum);
    if (pinRC != RC_OK)
    {
        free(pageHandle);
        return pinRC;
    }

    // Get the record data
    char *dtPtr = pageHandle->data;
    char *recPtr = dtPtr + (recSize * numberofSlot);

    if (record->data == NULL)
    {
        record->data = (char *)malloc(recSize);
        if (record->data == NULL)
        {
            unpinPage(bm, pageHandle);
            free(pageHandle);
            return RC_MEM_ALLOC_FAILURE;
        }
    }
    memcpy(record->data, recPtr, recSize);

    // Unpin the page
    RC unpinRC = unpinPage(bm, pageHandle);
    free(pageHandle);

    return unpinRC;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *condition)
{
    // Check for null inputs
    if (rel == NULL || scan == NULL || condition == NULL)
    {
        return RC_ERROR;
    }

    // Get total pages from file
    SM_FileHandle fileHandle;
    RC rc = openPageFile(rel->name, &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }
    int totalNumPages = fileHandle.totalNumPages;
    closePageFile(&fileHandle);

    // Calculate slots per page
    int recsize = getRecordSize(rel->schema);
    if (recsize <= 0)
    {
        return RC_ERROR;
    }
    int totNumSlots = PAGE_SIZE / recsize;

    // Initialize scan data
    ScanData *scanDataInfo = (ScanData *)malloc(sizeof(ScanData));
    if (scanDataInfo == NULL)
    {
        return RC_ERROR;
    }
    *scanDataInfo = (ScanData){
        .thisSlot = 0,
        .thisPage = 1,
        .numOfPages = totalNumPages,
        .totalNumSlots = totNumSlots,
        .theCondition = condition};

    (*scan).rel = rel;
    (*scan).mgmtData = scanDataInfo;

    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)
{
    // Check for null inputs
    if (!scan)
    {
        return RC_ERROR;
    }

    if (!record)
    {
        return RC_ERROR;
    }

    if (!scan->mgmtData)
    {
        return RC_ERROR;
    }
    ScanData *scaninformation = (ScanData *)scan->mgmtData;
    Value *value = NULL;
    RC rc;

    while (true)
    {
        // Check if it's reached the end of the table
        if (scaninformation->thisPage >= scaninformation->numOfPages)
        {
            return RC_RM_NO_MORE_TUPLES;
        }

        // Set record ID
        record->id.page = scaninformation->thisPage;
        record->id.slot = scaninformation->thisSlot;

        // Get record
        rc = getRecord(scan->rel, record->id, record);
        if (rc != RC_OK)
        {
            return rc;
        }

        // Validate record
        Value *idValue;
        rc = getAttr(record, scan->rel->schema, 0, &idValue);
        if (rc != RC_OK)
        {
            return rc;
        }

        // If the id is zero, mark the record as invalid and skip it
        if (idValue->v.intV == 0)
        {
            freeVal(idValue);
            scaninformation->thisSlot++;
            if (scaninformation->thisSlot >= scaninformation->totalNumSlots)
            {
                scaninformation->thisSlot = 0;
                scaninformation->thisPage++;
            }
            continue; // Skip to next record
        }

        freeVal(idValue); // Free after check

        // Evaluate condition
        rc = evalExpr(
            record,
            scan->rel->schema,
            scaninformation->theCondition,
            &value);
        if (rc != RC_OK)
        {
            return rc;
        }

        // Move to next slot/page
        scaninformation->thisSlot++;
        if (scaninformation->thisSlot >= scaninformation->totalNumSlots)
        {
            scaninformation->thisSlot = 0;
            scaninformation->thisPage++;
        }

        // If condition is true, return the record
        if (value->v.boolV)
        {
            freeVal(value);
            return RC_OK;
        }

        freeVal(value);
    }
}

RC closeScan(RM_ScanHandle *scan)
{
    // Check for null input
    if (!scan || !scan->mgmtData)
    {
        return RC_ERROR;
    }

    // Free scan management data
    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK;
}

extern int getRecordSize(Schema *schema)
{
    // Check for null input
    if (schema == NULL)
    {
        return RC_ERROR;
    }

    int recSize = 1;
    // Calculate record size based on attribute types
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            recSize += SIZE_INT;
            break;
        case DT_FLOAT:
            recSize += SIZE_FLOAT;
            break;
        case DT_STRING:
            recSize += schema->typeLength[i];
            break;
        case DT_BOOL:
            recSize += SIZE_BOOL;
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
        }
        if (i < schema->numAttr - 1)
        {
            recSize = recSize + 1; // Add separator
        }
    }
    return recSize;
}

RC freeSchema(Schema *schema)
{
    // Free all attribute names
    for (int i = 0; i < schema->numAttr; i++)
    {
        free(schema->attrNames[i]);
    }

    // Free schema attribute names, data types, type lengths, and key attributes
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);

    return RC_OK;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    // Check input parameters
    if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL || (keySize < 0 && keys == NULL))
    {
        return NULL;
    }

    // Allocate memory for schema
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL;
    }

    // Initialize schema components
    schema->numAttr = numAttr;
    schema->attrNames = malloc(numAttr * sizeof(char *));
    schema->dataTypes = malloc(numAttr * sizeof(DataType));
    schema->typeLength = malloc(numAttr * sizeof(int));
    schema->keyAttrs = malloc(keySize * sizeof(int));

    // Check if memory allocation was successful
    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL || schema->keyAttrs == NULL)
    {
        freeSchema(schema);
        return NULL;
    }

    // Copy attribute information
    for (int i = 0; i < numAttr; i++)
    {
        schema->attrNames[i] = strdup(attrNames[i]);
        if (schema->attrNames[i] == NULL)
        {
            free(schema);
            return NULL;
        }
        schema->dataTypes[i] = dataTypes[i];
        schema->typeLength[i] = typeLength[i];
    }

    // Copy key information
    schema->keySize = keySize;
    memcpy(schema->keyAttrs, keys, keySize * sizeof(int));

    return schema;
}

RC createRecord(Record **record, Schema *schema)
{
    // Check input parameters
    if (record == NULL || schema == NULL)
    {
        return RC_ERROR;
    }

    // Get record size
    int size = getRecordSize(schema);
    if (size <= 0)
    {
        return RC_ERROR;
    }

    // Allocate memory for record
    Record *r = (Record *)malloc(sizeof(Record));
    if (r == NULL)
    {
        return RC_ERROR;
    }

    // Allocate memory for record data
    r->data = calloc(size, 1); // char size
    if (r->data == NULL)
    {
        free(r);
        return RC_ERROR;
    }

    *record = r;
    return RC_OK;
}

RC freeRecord(Record *record)
{
    // Check if record is null
    if (record == NULL)
    {
        return RC_ERROR;
    }

    // Free record data if it exists
    if (record->data != NULL)
    {
        free(record->data);
        record->data = NULL;
    }

    // Free record
    free(record);
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    // Allocate memory for value
    Value *val = (Value *)malloc(sizeof(Value));
    if (val == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    // Calculate offset to attribute
    int offset = 0;
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            offset += SIZE_INT;
            break;
        case DT_FLOAT:
            offset += SIZE_FLOAT;
            break;
        case DT_BOOL:
            offset += SIZE_BOOL;
            break;
        case DT_STRING:
            offset += schema->typeLength[i];
            break;
        }
    }

    offset += attrNum + 1;

    // Get pointer to attribute data
    char *source = record->data + offset;
    val->dt = schema->dataTypes[attrNum];

    // get attribute value based on data type
    switch (val->dt)
    {
    case DT_INT:
        val->v.intV = atoi(source);
        break;
    case DT_FLOAT:
        val->v.floatV = atof(source);
        break;
    case DT_BOOL:
        val->v.boolV = (*source != '0');
        break;
    case DT_STRING:
        val->v.stringV = (char *)calloc(schema->typeLength[attrNum] + 1, sizeof(char));
        if (val->v.stringV == NULL)
        {
            return RC_MEM_ALLOC_FAILURE;
        }
        strncpy(val->v.stringV, source, schema->typeLength[attrNum]);
        val->v.stringV[schema->typeLength[attrNum]] = '\0';
        break;
    default:
        return RC_RM_UNKOWN_DATATYPE;
    }

    *value = val;
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    // Check if attr no. is valid
    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_NULL_PARAM;
    }

    // Calculate offset to attr
    int offset = 0;
    size_t attributeIndex;
    const int extraSpaceOffset = 1;
    offset += attrNum + extraSpaceOffset;

    for (attributeIndex = 0; attributeIndex < attrNum; attributeIndex++)
    {
        switch (schema->dataTypes[attributeIndex])
        {
        case DT_INT:
            offset += SIZE_INT;
            break;
        case DT_FLOAT:
            offset += SIZE_FLOAT;
            break;
        case DT_BOOL:
            offset += SIZE_BOOL;
            break;
        case DT_STRING:
            offset += schema->typeLength[attributeIndex];
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
        }
    }

    // Get pointer to attr data
    char *output = record->data;
    output += offset;

    // Set delimiter
    *(output - 1) = (attrNum == 0) ? DELIMITER_FIRST_ATTR : DELIMITER_OTHER_ATTR;

    // Set attr value
    switch (value->dt)
    {
    case DT_INT:
        sprintf(output, "%04d", value->v.intV);
        break;
    case DT_FLOAT:
        sprintf(output, "%04f", value->v.floatV);
        break;
    case DT_BOOL:
        sprintf(output, "%i", value->v.boolV);
        break;
    case DT_STRING:
        sprintf(output, "%s", value->v.stringV);
        break;
    default:
        return RC_RM_UNKOWN_DATATYPE;
    }

    return RC_OK;
}