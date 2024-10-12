#include "record_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "expr.h"
#include <stdlib.h>
#include <string.h>

#define MAX_PAGE_FILE_NAME 255
#define SIZE_INT sizeof(int)
#define SIZE_FLOAT sizeof(float)
#define SIZE_BOOL sizeof(bool)
#define DELIMITER_FIRST_ATTR '|'
#define DELIMITER_OTHER_ATTR ','

static char pageFile[MAX_PAGE_FILE_NAME];

typedef struct ScanData
{
    int currentPage;
    int currentSlot;
    int numOfPages;
    int totalNumSlots;
    Expr *condition;
} ScanData;

RC initRecordManager(void *mgmtData)
{
    return RC_OK;
}

RC shutdownRecordManager()
{
    return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
    RC rc;
    SM_FileHandle fileHandle;
    char *schemaToString = NULL;

    if (name == NULL || schema == NULL)
    {
        return RC_NULL_PARAM;
    }

    if (strlen(name) >= MAX_PAGE_FILE_NAME)
    {
        return RC_NAME_TOO_LONG;
    }
    strncpy(pageFile, name, MAX_PAGE_FILE_NAME);
    pageFile[MAX_PAGE_FILE_NAME - 1] = '\0';

    rc = createPageFile(name);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = openPageFile(name, &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = ensureCapacity(1, &fileHandle);
    if (rc != RC_OK)
    {
        closePageFile(&fileHandle);
        return rc;
    }

    schemaToString = serializeSchema(schema);
    if (schemaToString == NULL)
    {
        closePageFile(&fileHandle);
        return RC_SERIALIZATION_ERROR;
    }

    rc = writeBlock(0, &fileHandle, schemaToString);
    free(schemaToString);

    if (rc != RC_OK)
    {
        closePageFile(&fileHandle);
        return rc;
    }

    rc = closePageFile(&fileHandle);
    return rc;
}

Schema *deserializeSchema(char *serializedSchema)
{
    if (serializedSchema == NULL)
    {
        return NULL;
    }

    Schema *schemaResult = (Schema *)malloc(sizeof(Schema));
    if (schemaResult == NULL)
    {
        return NULL;
    }

    char *schemaData = strdup(serializedSchema);
    if (schemaData == NULL)
    {
        free(schemaResult);
        return NULL;
    }

    char *tokenPointer;
    char *tmpStr = strtok_r(schemaData, "<", &tokenPointer);
    tmpStr = strtok_r(NULL, ">", &tokenPointer);
    schemaResult->numAttr = atoi(tmpStr);

    schemaResult->attrNames = (char **)malloc(sizeof(char *) * schemaResult->numAttr);
    schemaResult->dataTypes = (DataType *)malloc(sizeof(DataType) * schemaResult->numAttr);
    schemaResult->typeLength = (int *)malloc(sizeof(int) * schemaResult->numAttr);

    if (!schemaResult->attrNames || !schemaResult->dataTypes || !schemaResult->typeLength)
    {
        free(schemaData);
        free(schemaResult->attrNames);
        free(schemaResult->dataTypes);
        free(schemaResult->typeLength);
        free(schemaResult);
        return NULL;
    }

    tmpStr = strtok_r(NULL, "(", &tokenPointer);
    for (int i = 0; i < schemaResult->numAttr; i++)
    {
        tmpStr = strtok_r(NULL, ": ", &tokenPointer);
        schemaResult->attrNames[i] = strdup(tmpStr);
        if (schemaResult->attrNames[i] == NULL)
        {
            for (int j = 0; j < i; j++)
            {
                free(schemaResult->attrNames[j]);
            }
            free(schemaData);
            free(schemaResult->attrNames);
            free(schemaResult->dataTypes);
            free(schemaResult->typeLength);
            free(schemaResult);
            return NULL;
        }

        tmpStr = strtok_r(NULL, i == schemaResult->numAttr - 1 ? ") " : ", ", &tokenPointer);

        if (strcmp(tmpStr, "INT") == 0)
        {
            schemaResult->dataTypes[i] = DT_INT;
            schemaResult->typeLength[i] = 0;
        }
        else if (strcmp(tmpStr, "FLOAT") == 0)
        {
            schemaResult->dataTypes[i] = DT_FLOAT;
            schemaResult->typeLength[i] = 0;
        }
        else if (strcmp(tmpStr, "BOOL") == 0)
        {
            schemaResult->dataTypes[i] = DT_BOOL;
            schemaResult->typeLength[i] = 0;
        }
        else
        {
            schemaResult->dataTypes[i] = DT_STRING;
            char *tokenPointer2;
            char *token = strtok_r(tmpStr, "[", &tokenPointer2);
            token = strtok_r(NULL, "]", &tokenPointer2);
            schemaResult->typeLength[i] = atoi(token);
        }
    }

    int keySize = 0;
    char *keyAttr[schemaResult->numAttr];

    tmpStr = strtok_r(NULL, "(", &tokenPointer);
    if (tmpStr != NULL)
    {
        tmpStr = strtok_r(NULL, ")", &tokenPointer);
        char *tokenPointer2;

        char *tmpKey = strtok_r(tmpStr, ", ", &tokenPointer2);

        while (tmpKey != NULL)
        {
            keyAttr[keySize] = strdup(tmpKey);
            if (keyAttr[keySize] == NULL)
            {
                for (int k = 0; k < keySize; k++)
                {
                    free(keyAttr[k]);
                }
                for (int j = 0; j < schemaResult->numAttr; j++)
                {
                    free(schemaResult->attrNames[j]);
                }
                free(schemaData);
                free(schemaResult->attrNames);
                free(schemaResult->dataTypes);
                free(schemaResult->typeLength);
                free(schemaResult);
                return NULL;
            }
            keySize++;
            tmpKey = strtok_r(NULL, ", ", &tokenPointer2);
        }
    }

    if (keySize > 0)
    {
        schemaResult->keyAttrs = (int *)malloc(sizeof(int) * keySize);
        if (schemaResult->keyAttrs == NULL)
        {
            for (int k = 0; k < keySize; k++)
            {
                free(keyAttr[k]);
            }
            for (int j = 0; j < schemaResult->numAttr; j++)
            {
                free(schemaResult->attrNames[j]);
            }
            free(schemaData);
            free(schemaResult->attrNames);
            free(schemaResult->dataTypes);
            free(schemaResult->typeLength);
            free(schemaResult);
            return NULL;
        }

        schemaResult->keySize = keySize;

        for (int j = 0; j < keySize; j++)
        {
            for (int z = 0; z < schemaResult->numAttr; z++)
            {
                if (strcmp(schemaResult->attrNames[z], keyAttr[j]) == 0)
                {
                    schemaResult->keyAttrs[j] = z;
                    break;
                }
            }
            free(keyAttr[j]);
        }
    }
    else
    {
        schemaResult->keyAttrs = NULL;
        schemaResult->keySize = 0;
    }

    free(schemaData);
    return schemaResult;
}
RC openTable(RM_TableData *rel, char *name)
{
    if (!rel || !name)
        return RC_NULL_PARAM;
    BM_BufferPool *bm = malloc(sizeof(BM_BufferPool));
    BM_PageHandle *pageHandle = malloc(sizeof(BM_PageHandle));
    if (!bm || !pageHandle)
        return RC_MEM_ALLOC_FAILURE;
    RC rc;
    rc = initBufferPool(bm, name, 3, RS_FIFO, NULL);
    if (rc != RC_OK)
        goto cleanup;
    rc = pinPage(bm, pageHandle, 0);
    if (rc != RC_OK)
        goto cleanup;
    Schema *deserializedSchema = deserializeSchema(pageHandle->data);
    unpinPage(bm, pageHandle);
    if (!deserializedSchema)
    {
        rc = RC_SCHEMA_DESERIALIZATION_FAIL;
        goto cleanup;
    }
    rel->name = name;
    rel->mgmtData = bm;
    rel->schema = deserializedSchema;

cleanup:

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
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    shutdownBufferPool(bm);
    for (int i = 0; i < rel->schema->numAttr; i++)
    {
        free(rel->schema->attrNames[i]);
    }
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->typeLength);
    free(rel->schema->keyAttrs);
    free(rel->schema);
    free(rel->mgmtData);
    return RC_OK;
}

RC deleteTable(char *name)
{

    if (!name)
        return RC_NULL_PARAM;
    if (remove(name) != 0)
    {
        return RC_RM_TABLE_NOT_EXIST;
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
    if (openPageFile(pageFile, &fileHandle) != RC_OK)
    {
        return -1;
    }
    int blockNum = 1, totalRecord = 0;
    while (blockNum < fileHandle.totalNumPages)
    {
        if (pinPage(bm, &pageHandle, blockNum) != RC_OK)
        {
            closePageFile(&fileHandle);
            return -1;
        }
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

RC insertRecord(RM_TableData *rel, Record *record)
{
    // Declaring variables
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    SM_FileHandle fileHandle;
    PageNumber pageNum;
    int slotNum = 0;
    int pageLength = 0;

    openPageFile(pageFile, &fileHandle);
    int totalNumPages = fileHandle.totalNumPages;
    closePageFile(&fileHandle);
    int recordSize = getRecordSize(rel->schema) + 3;
    pageNum = 1;

    while (totalNumPages > pageNum)
    {
        pinPage(bm, pageHandle, pageNum);
        char *pageData = pageHandle->data;
        pageLength = strlen(pageData);
        int remainingSpace = PAGE_SIZE - pageLength;

        if (recordSize < remainingSpace)
        {
            slotNum = pageLength / recordSize;
            unpinPage(bm, pageHandle);
            break;
        }

        unpinPage(bm, pageHandle);
        pageNum++;
    }

    pinPage(bm, pageHandle, pageNum);
    char *dataPointer = pageHandle->data;
    pageLength = strlen(dataPointer);
    char *recordPointer = pageLength + dataPointer;
    strcpy(recordPointer, record->data);
    markDirty(bm, pageHandle);
    unpinPage(bm, pageHandle);
    RID recordID;
    recordID.page = pageNum;
    recordID.slot = slotNum;
    record->id = recordID;

    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    if (rel == NULL || record == NULL)
    {
        return RC_NULL_PARAM;
    }
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle pageHandle;
    PageNumber pageNum = record->id.page;
    int slotNum = record->id.slot;
    int recordSize = getRecordSize(rel->schema) + 3;
    RC rc = pinPage(bm, &pageHandle, pageNum);
    if (rc != RC_OK)
    {
        return rc;
    }

    char *dataPointer = pageHandle.data + slotNum * recordSize;
    strncpy(dataPointer, record->data, recordSize);
    markDirty(bm, &pageHandle);
    unpinPage(bm, &pageHandle);
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    if (rel == NULL || record == NULL)
    {
        return RC_NULL_PARAM;
    }
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if (pageHandle == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    PageNumber pageNum = id.page;
    int slotNum = id.slot;
    int recordSize = getRecordSize(rel->schema) + 3;
    RC pinRC = pinPage(bm, pageHandle, pageNum);
    if (pinRC != RC_OK)
    {
        free(pageHandle);
        return pinRC;
    }

    char *dataPointer = pageHandle->data;
    char *recordPointer = dataPointer + (recordSize * slotNum);

    if (record->data == NULL)
    {
        record->data = (char *)malloc(recordSize);
        if (record->data == NULL)
        {
            unpinPage(bm, pageHandle);
            free(pageHandle);
            return RC_MEM_ALLOC_FAILURE;
        }
    }
    memcpy(record->data, recordPointer, recordSize);
    RC unpinRC = unpinPage(bm, pageHandle);
    free(pageHandle);

    return unpinRC;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if (!rel || !scan || !cond)
    {
        return RC_ERROR;
    }

    SM_FileHandle fileHandle;
    RC rc;
    rc = openPageFile(rel->name, &fileHandle);
    if (rc != RC_OK)
    {
        return rc;
    }

    int totalNumPages = fileHandle.totalNumPages;
    closePageFile(&fileHandle);
    int recordSize = getRecordSize(rel->schema);
    if (recordSize <= 0)
    {
        return RC_ERROR;
    }
    int totalNumSlots = PAGE_SIZE / recordSize;
    ScanData *scanInfo = (ScanData *)malloc(sizeof(ScanData));
    if (!scanInfo)
    {
        return RC_ERROR;
    }

    scanInfo->currentSlot = 0;
    scanInfo->currentPage = 1;
    scanInfo->numOfPages = totalNumPages;
    scanInfo->totalNumSlots = totalNumSlots;
    scanInfo->condition = cond;

    scan->rel = rel;
    scan->mgmtData = scanInfo;

    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)
{
    if (!scan || !record || !scan->mgmtData)
    {
        return RC_ERROR;
    }

    ScanData *scanInfo = (ScanData *)scan->mgmtData;
    Value *value = NULL;
    RC rc;

    while (true)
    {
        if (scanInfo->currentPage >= scanInfo->numOfPages)
        {
            return RC_RM_NO_MORE_TUPLES;
        }
        record->id.page = scanInfo->currentPage;
        record->id.slot = scanInfo->currentSlot;

        rc = getRecord(scan->rel, record->id, record);
        if (rc != RC_OK)
        {
            return rc;
        }

        rc = evalExpr(record, scan->rel->schema, scanInfo->condition, &value);
        if (rc != RC_OK)
        {
            return rc;
        }

        scanInfo->currentSlot++;
        if (scanInfo->currentSlot >= scanInfo->totalNumSlots)
        {
            scanInfo->currentSlot = 0;
            scanInfo->currentPage++;
        }

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
    if (!scan || !scan->mgmtData)
    {
        return RC_ERROR;
    }

    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK;
}

extern int getRecordSize(Schema *schema)
{
    if (schema == NULL)
    {
        return RC_ERROR;
    }

    int size = 0;
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            size += sizeof(int);
            break;
        case DT_STRING:
            size += schema->typeLength[i];
            break;
        case DT_FLOAT:
            size += sizeof(float);
            break;
        case DT_BOOL:
            size += sizeof(bool);
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
        }
    }
    return size;
}

RC freeSchema(Schema *schema)
{
    for (int i = 0; i < schema->numAttr; i++)
    {
        free(schema->attrNames[i]);
    }

    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);

    return RC_OK;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL || keySize < 0 || keys == NULL)
    {
        return NULL;
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL;
    }

    schema->numAttr = numAttr;
    schema->attrNames = malloc(numAttr * sizeof(char *));
    schema->dataTypes = malloc(numAttr * sizeof(DataType));
    schema->typeLength = malloc(numAttr * sizeof(int));
    schema->keyAttrs = malloc(keySize * sizeof(int));

    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL || schema->keyAttrs == NULL)
    {
        freeSchema(schema);
        return NULL;
    }

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

    schema->keySize = keySize;
    memcpy(schema->keyAttrs, keys, keySize * sizeof(int));

    return schema;
}

RC createRecord(Record **record, Schema *schema)
{
    if (record == NULL || schema == NULL)
    {
        return RC_ERROR;
    }

    int size = getRecordSize(schema);
    if (size <= 0)
    {
        return RC_ERROR;
    }

    Record *r = (Record *)malloc(sizeof(Record));
    if (r == NULL)
    {
        return RC_ERROR;
    }

    r->data = (char *)calloc(size, sizeof(char));
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
    if (record == NULL)
    {
        return RC_ERROR;
    }

    if (record->data != NULL)
    {
        free(record->data);
        record->data = NULL;
    }

    free(record);
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    Value *val = (Value *)malloc(sizeof(Value));
    if (val == NULL)
    {
        return RC_MEM_ALLOC_FAILURE;
    }

    int offset = 0;
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
        case DT_STRING:
            offset += schema->typeLength[i];
            break;
        }
    }

    offset += attrNum + 1;

    char *source = record->data + offset;
    val->dt = schema->dataTypes[attrNum];

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
    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_NULL_PARAM;
    }

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

    char *output = record->data;
    output += offset;

    *(output - 1) = (attrNum == 0) ? DELIMITER_FIRST_ATTR : DELIMITER_OTHER_ATTR;
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
