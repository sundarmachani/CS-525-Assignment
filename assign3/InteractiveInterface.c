#include <stdio.h>
#include "record_mgr.h"
#include <stdlib.h>
#include "expr.h"
#include <string.h>

#define ATTRIBUTE_NAME_SIZE 100
#define MAX_PAGE_FILE_NAME 255
char tableName[MAX_PAGE_FILE_NAME];
#define DELIMITER_FIRST_ATTR '|'
#define DELIMITER_OTHER_ATTR ','

void interactiveInterface(RM_TableData *table);
void printValue(Value *value);

int main()
{
    // Initialize the record manager
    initRecordManager(NULL);

    // Example table setup
    RM_TableData *table = (RM_TableData *)malloc(sizeof(RM_TableData));

    // Start the interactive interface
    interactiveInterface(table);

    // Clean up
    free(table);

    // Shutdown the record manager
    shutdownRecordManager();

    return 0;
}

void createTableInterface()
{
    int numAttributes;

    printf("Enter table name: ");
    scanf("%s", tableName);

    printf("Enter number of attributes: ");
    scanf("%d", &numAttributes);

    char *attrNames[numAttributes];
    DataType dataTypes[numAttributes];
    int typeLengths[numAttributes];

    for (int i = 0; i < numAttributes; i++)
    {
        attrNames[i] = (char *)malloc(ATTRIBUTE_NAME_SIZE);
        printf("Enter name for attribute %d: ", i + 1);
        scanf("%s", attrNames[i]);

        printf("Enter data type for attribute %d (0: INT, 1: STRING, 2: FLOAT, 3: BOOL): ", i + 1);
        int dtype;
        scanf("%d", &dtype);

        // Validate data type input
        if (dtype < 0 || dtype > 3)
        {
            printf("Invalid data type! Please enter a valid option.\n");
            i--; // Redo this iteration
            continue;
        }

        dataTypes[i] = (DataType)dtype;

        // Only ask for length if the data type is STRING
        if (dataTypes[i] == DT_STRING)
        {
            printf("Enter length for string attribute \"%s (%d)\": ", attrNames[i], i + 1);
            scanf("%d", &typeLengths[i]);
        }
        else
        {
            typeLengths[i] = 0; // Set length to 0 for non-string types
        }

        // Clear input buffer
        while (getchar() != '\n')
            ;
    }

    int keys[] = {0};
    int *cpKeys = (int *)malloc(sizeof(int));
    memcpy(cpKeys, keys, sizeof(int));
    // Create schema and table
    Schema *schema = createSchema(numAttributes, attrNames, dataTypes, typeLengths, 1, cpKeys);
    if (schema == NULL)
    {
        printf("Error: Failed to create schema.\n");
        // Free allocated memory
        for (int i = 0; i < numAttributes; i++)
        {
            free(attrNames[i]);
        }
        // free(keys);
        return;
    }

    RC rc = createTable(tableName, schema);

    // Enhanced error handling
    if (rc == RC_OK)
    {
        printf("Table created successfully.\n");
    }
    else
    {
        printf("Error creating table. Error code: %d\n", rc);
    }

    // Free allocated memory
    for (int i = 0; i < numAttributes; i++)
    {
        free(attrNames[i]);
    }
    // free(keys);
    freeSchema(schema);
}

void insertRecordInterface(RM_TableData *table)
{
    if (tableName[0] == '\0')
    {
        char useExisting;
        printf("No table currently selected. Do you want to use an existing table? (y/n): ");
        scanf(" %c", &useExisting);

        if (useExisting == 'y' || useExisting == 'Y')
        {
            printf("Enter the name of the existing table: ");
            scanf("%s", tableName);
        }
        else if (useExisting == 'n' || useExisting == 'N')
        {
            printf("Error: Please create a table first.\n");
            return;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
            return;
        }
        return;
    }

    RC rc = openTable(table, tableName);
    if (rc != RC_OK)
    {
        printf("Error: Unable to open table '%s'. Error code: %d\n", tableName, rc);
        return;
    }
    Record *record;
    createRecord(&record, table->schema);

    char *recordData = record->data;
    int offset = 0;

    for (int i = 0; i < table->schema->numAttr; i++)
    {
        Value *value = (Value *)malloc(sizeof(Value));
        char input[100];
        int valid_input = 0;

        while (!valid_input)
        {
            if (table->schema->dataTypes[i] == DT_FLOAT)
            {
                printf("Enter new value for %s maximum 6 characters: ", table->schema->attrNames[i]);
            }
            else if (table->schema->dataTypes[i] == DT_INT)
            {
                printf("Enter new value for %s maximum 4 characters: ", table->schema->attrNames[i]);
            }
            else if (table->schema->dataTypes[i] == DT_BOOL)
            {
                printf("Enter new value for %s (0 or 1): ", table->schema->attrNames[i]);
            }
            else
            {
                printf("Enter new value for %s: ", table->schema->attrNames[i]);
            }

            scanf("%99s", input);

            switch (table->schema->dataTypes[i])
            {
            case DT_INT:
                if (strlen(input) <= 4 && sscanf(input, "%d", &value->v.intV) == 1)
                {
                    value->dt = DT_INT;
                    sprintf(recordData + offset, "%c%04d", (i == 0) ? '|' : ',', value->v.intV);
                    offset += 5;
                    valid_input = 1;
                }
                else
                {
                    printf("Invalid input. Please enter an integer with maximum 4 characters.\n");
                }
                break;
            case DT_FLOAT:
                if (strlen(input) <= 8 && sscanf(input, "%f", &value->v.floatV) == 1)
                {
                    value->dt = DT_FLOAT;
                    sprintf(recordData + offset, "%c%f", (i == 0) ? '|' : ',', value->v.floatV);
                    offset += strlen(recordData + offset);
                    valid_input = 1;
                }
                else
                {
                    printf("Invalid input. Please enter a float with maximum 8 characters.\n");
                }
                break;
            case DT_BOOL:
                if ((strcmp(input, "0") == 0) || (strcmp(input, "1") == 0))
                {
                    value->v.boolV = (strcmp(input, "1") == 0) ? 1 : 0;
                    value->dt = DT_BOOL;
                    sprintf(recordData + offset, "%c%d", (i == 0) ? '|' : ',', value->v.boolV);
                    offset += 2;
                    valid_input = 1;
                }
                else
                {
                    printf("Invalid input. Please enter 0 for false or 1 for true.\n");
                }
                break;
            case DT_STRING:
                if (strlen(input) <= table->schema->typeLength[i])
                {
                    value->v.stringV = (char *)malloc(table->schema->typeLength[i] + 1);
                    strcpy(value->v.stringV, input);
                    value->v.stringV[table->schema->typeLength[i]] = '\0';
                    value->dt = DT_STRING;
                    sprintf(recordData + offset, "%c%-*.*s",
                            (i == 0) ? '|' : ',',
                            table->schema->typeLength[i],
                            table->schema->typeLength[i],
                            value->v.stringV);
                    offset += table->schema->typeLength[i] + 1;
                    valid_input = 1;
                }
                else
                {
                    printf("Invalid input. Please enter a string with maximum %d characters.\n", table->schema->typeLength[i]);
                }
                break;
            }
        }

        free(value);
    }

    rc = insertRecord(table, record);
    if (rc == RC_OK)
    {
        printf("Record inserted successfully.\n");
        printf("Inserted record data: %s\n", record->data);
    }
    else
    {
        printf("Error inserting record: %d\n", rc);
    }

    freeRecord(record);
    closeTable(table);
}

// Helper function to select an attribute and get a search value
Value *selectAttributeAndGetValue(RM_TableData *table, int *selectedAttr)
{
    // Display available attributes
    printf("Available attributes in the table \"%s\" are:\n", table->name);
    for (int i = 0; i < table->schema->numAttr; i++)
    {
        printf("%d: %s\n", i, table->schema->attrNames[i]);
    }

    // Let user choose attribute
    printf("Enter the number of the attribute: ");
    scanf("%d", selectedAttr);

    if (*selectedAttr < 0 || *selectedAttr >= table->schema->numAttr)
    {
        printf("Invalid attribute number.\n");
        return NULL;
    }

    // Get value from user
    Value *value = (Value *)malloc(sizeof(Value));
    printf("Enter the value: ");
    switch (table->schema->dataTypes[*selectedAttr])
    {
    case DT_INT:
        while (scanf("%d", &value->v.intV) != 1)
        {
            printf("Invalid input. Please enter an integer: ");
            while (getchar() != '\n')
                ;
        }
        value->dt = DT_INT;
        break;
    case DT_FLOAT:
        while (scanf("%f", &value->v.floatV) != 1)
        {
            printf("Invalid input. Please enter a float: ");
            while (getchar() != '\n')
                ;
        }
        value->dt = DT_FLOAT;
        break;
    case DT_BOOL:
        while (scanf("%hd", &value->v.boolV) != 1 ||
               (value->v.boolV != 0 && value->v.boolV != 1))
        {
            printf("Invalid input. Please enter 0 for false or 1 for true: ");
            while (getchar() != '\n')
                ;
        }
        value->dt = DT_BOOL;
        break;
    case DT_STRING:
        value->v.stringV = (char *)malloc(table->schema->typeLength[*selectedAttr] + 1);
        scanf("%s", value->v.stringV);
        value->dt = DT_STRING;
        break;
    }

    return value;
}

void updateRecordInterface(RM_TableData *table)
{
    if (tableName[0] == '\0')
    {
        char useExisting;
        printf("No table currently selected. Do you want to use an existing table? (y/n): ");
        scanf(" %c", &useExisting);

        if (useExisting == 'y' || useExisting == 'Y')
        {
            printf("Enter the name of the existing table: ");
            scanf("%s", tableName);
        }
        else if (useExisting == 'n' || useExisting == 'N')
        {
            printf("Error: Please create a table first.\n");
            return;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
            return;
        }
    }

    RC rc = openTable(table, tableName);
    if (rc != RC_OK)
    {
        printf("Error: Unable to open table '%s'. Error code: %d\n", tableName, rc);
        return;
    }

    int searchAttrNum;
    Value *searchValue = selectAttributeAndGetValue(table, &searchAttrNum);
    if (searchValue == NULL)
    {
        closeTable(table);
        return;
    }

    RM_ScanHandle scanHandle;
    Expr *condition = NULL, *left, *right;
    MAKE_CONS(left, searchValue);
    MAKE_ATTRREF(right, searchAttrNum);
    MAKE_BINOP_EXPR(condition, left, right, OP_COMP_EQUAL);

    rc = startScan(table, &scanHandle, condition);
    if (rc != RC_OK)
    {
        printf("Error starting scan. RC: %d\n", rc);
        freeVal(searchValue);
        closeTable(table);
        return;
    }

    Record *record = (Record *)malloc(sizeof(Record));
    createRecord(&record, table->schema);

    bool recordFound = false;
    while (next(&scanHandle, record) == RC_OK)
    {
        // Check if the record is valid (not deleted)
        if (record->data[0] == '\0') // Assuming '\0' indicates a deleted record
        {
            continue; // Skip this record
        }
        recordFound = true;
        printf("Found matching record. Current values:\n");

        char *recordData = strdup(record->data);
        if (recordData == NULL)
        {
            printf("Error: Unable to allocate memory for record data.\n");
            continue;
        }

        char *token;
        char *rest = recordData;

        for (int i = 0; i < table->schema->numAttr; i++)
        {
            if (i == 0)
            {
                token = strtok_r(rest, ",", &rest);
                if (token != NULL && token[0] == '|')
                {
                    token++; // Skip the '|'
                }
            }
            else
            {
                token = strtok_r(NULL, ",", &rest);
            }

            if (token == NULL)
            {
                printf("Error parsing record data for attribute %d\n", i);
                break;
            }

            printf("%s: ", table->schema->attrNames[i]);

            switch (table->schema->dataTypes[i])
            {
            case DT_INT:
                printf("%d", atoi(token));
                break;
            case DT_FLOAT:
                printf("%f", atof(token));
                break;
            case DT_BOOL:
                printf("%s", strcmp(token, "0") == 0 ? "false" : "true");
                break;
            case DT_STRING:
                printf("%s", token);
                break;
            default:
                printf("Unknown data type");
            }
            printf("\n");
        }
        free(recordData);

        char updateChoice;
        printf("Do you want to update this record? (y/n): ");
        scanf(" %c", &updateChoice);

        if (updateChoice == 'y' || updateChoice == 'Y')
        {
            char *recordData = record->data;
            int offset = 0;
            for (int i = 0; i < table->schema->numAttr; i++)
            {
                char input[100];
                int valid_input = 0;

                while (!valid_input)
                {
                    if (table->schema->dataTypes[i] == DT_FLOAT)
                    {
                        printf("Enter new value for %s maximum 8 characters: ", table->schema->attrNames[i]);
                    }
                    else if (table->schema->dataTypes[i] == DT_INT)
                    {
                        printf("Enter new value for %s maximum 4 characters: ", table->schema->attrNames[i]);
                    }
                    else if (table->schema->dataTypes[i] == DT_BOOL)
                    {
                        printf("Enter new value for %s (0 or 1): ", table->schema->attrNames[i]);
                    }
                    else
                    {
                        printf("Enter new value for %s: ", table->schema->attrNames[i]);
                    }

                    scanf("%99s", input);

                    switch (table->schema->dataTypes[i])
                    {
                    case DT_INT:
                        if (strlen(input) <= 4 && sscanf(input, "%d", &((Value *)&input)->v.intV) == 1)
                        {
                            sprintf(recordData + offset, "%c%04d", (i == 0) ? '|' : ',', ((Value *)&input)->v.intV);
                            offset += 5;
                            valid_input = 1;
                        }
                        else
                        {
                            printf("Invalid input. Please enter an integer with maximum 4 characters.\n");
                        }
                        break;
                    case DT_FLOAT:
                        if (strlen(input) <= 8 && sscanf(input, "%f", &((Value *)&input)->v.floatV) == 1)
                        {
                            sprintf(recordData + offset, "%c%f", (i == 0) ? '|' : ',', ((Value *)&input)->v.floatV);
                            offset += strlen(recordData + offset);
                            valid_input = 1;
                        }
                        else
                        {
                            printf("Invalid input. Please enter a float with maximum 8 characters.\n");
                        }
                        break;
                    case DT_BOOL:
                        if ((strcmp(input, "0") == 0) || (strcmp(input, "1") == 0))
                        {
                            ((Value *)&input)->v.boolV = (strcmp(input, "1") == 0) ? 1 : 0;
                            sprintf(recordData + offset, "%c%d", (i == 0) ? '|' : ',', ((Value *)&input)->v.boolV);
                            offset += 2;
                            valid_input = 1;
                        }
                        else
                        {
                            printf("Invalid input. Please enter 0 for false or 1 for true.\n");
                        }
                        break;
                    case DT_STRING:
                        if (strlen(input) <= table->schema->typeLength[i])
                        {
                            sprintf(recordData + offset, "%c%-*.*s",
                                    (i == 0) ? '|' : ',',
                                    table->schema->typeLength[i],
                                    table->schema->typeLength[i],
                                    input);
                            offset += table->schema->typeLength[i] + 1;
                            valid_input = 1;
                        }
                        else
                        {
                            printf("Invalid input. Please enter a string with maximum %d characters.\n", table->schema->typeLength[i]);
                        }
                        break;
                    }
                }
            }

            rc = updateRecord(table, record);
            if (rc == RC_OK)
            {
                printf("Record updated successfully.\n");
            }
            else
            {
                printf("Error updating record. RC: %d\n", rc);
            }
        }
        else if (updateChoice == 'n' || updateChoice == 'N')
        {
            break;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
        }
    }

    if (!recordFound)
    {
        printf("No matching records found.\n");
    }

    closeScan(&scanHandle);
    freeRecord(record);
    freeVal(searchValue);
    closeTable(table);
}

void printValue(Value *value)
{
    if (value == NULL)
    {
        printf("NULL");
        return;
    }

    switch (value->dt)
    {
    case DT_INT:
        printf("%d", value->v.intV);
        break;
    case DT_FLOAT:
        printf("%f", value->v.floatV);
        break;
    case DT_BOOL:
        printf("%s", value->v.boolV ? "true" : "false");
        break;
    case DT_STRING:
        printf("%s", value->v.stringV);
        break;
    default:
        printf("Unknown data type");
    }
}

void deleteRecordInterface(RM_TableData *table)
{
    if (tableName[0] == '\0')
    {
        char useExisting;
        printf("No table currently selected. Do you want to use an existing table? (y/n): ");
        scanf(" %c", &useExisting);

        if (useExisting == 'y' || useExisting == 'Y')
        {
            printf("Enter the name of the existing table: ");
            scanf("%s", tableName);
        }
        else if (useExisting == 'n' || useExisting == 'N')
        {
            printf("Error: Please create a table first.\n");
            return;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
            return;
        }
    }

    RC rc = openTable(table, tableName);
    if (rc != RC_OK)
    {
        printf("Error: Unable to open table '%s'. Error code: %d\n", tableName, rc);
        return;
    }

    int searchAttrNum;
    Value *searchValue = selectAttributeAndGetValue(table, &searchAttrNum);
    if (searchValue == NULL)
    {
        closeTable(table);
        return;
    }

    RM_ScanHandle scanHandle;
    Expr *condition = NULL, *left, *right;
    MAKE_CONS(left, searchValue);
    MAKE_ATTRREF(right, searchAttrNum);
    MAKE_BINOP_EXPR(condition, left, right, OP_COMP_EQUAL);

    rc = startScan(table, &scanHandle, condition);
    if (rc != RC_OK)
    {
        printf("Error starting scan. RC: %d\n", rc);
        freeVal(searchValue);
        closeTable(table);
        return;
    }

    Record *record = (Record *)malloc(sizeof(Record));
    createRecord(&record, table->schema);

    bool recordFound = false;
    while (next(&scanHandle, record) == RC_OK)
    {
        // Check if the record is valid (not deleted)
        if (record->data[0] == '\0') // Assuming '\0' indicates a deleted record
        {
            continue; // Skip this record
        }
        recordFound = true;
        printf("Found matching record. Current values:\n");

        char *recordData = strdup(record->data);
        if (recordData == NULL)
        {
            printf("Error: Unable to allocate memory for record data.\n");
            continue;
        }

        char *token;
        char *rest = recordData;

        for (int i = 0; i < table->schema->numAttr; i++)
        {
            if (i == 0)
            {
                token = strtok_r(rest, ",", &rest);
                if (token != NULL && token[0] == '|')
                {
                    token++; // Skip the '|'
                }
            }
            else
            {
                token = strtok_r(NULL, ",", &rest);
            }

            if (token == NULL)
            {
                printf("Error parsing record data for attribute %d\n", i);
                break;
            }

            printf("%s: ", table->schema->attrNames[i]);

            switch (table->schema->dataTypes[i])
            {
            case DT_INT:
                printf("%d", atoi(token));
                break;
            case DT_FLOAT:
                printf("%f", atof(token));
                break;
            case DT_BOOL:
                printf("%s", strcmp(token, "0") == 0 ? "false" : "true");
                break;
            case DT_STRING:
                printf("%s", token);
                break;
            default:
                printf("Unknown data type");
            }
            printf("\n");
        }
        free(recordData);

        char deleteChoice;
        printf("Do you want to delete this record? (y/n): ");
        scanf(" %c", &deleteChoice);

        if (deleteChoice == 'y' || deleteChoice == 'Y')
        {
            rc = deleteRecord(table, record->id);
            if (rc == RC_OK)
            {
                printf("Record deleted successfully.\n");
            }
            else
            {
                printf("Error deleting record. RC: %d\n", rc);
            }
        }
        else if (deleteChoice == 'n' || deleteChoice == 'N')
        {
            break;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
        }
    }

    if (!recordFound)
    {
        printf("No matching records found.\n");
    }

    closeScan(&scanHandle);
    freeRecord(record);
    freeVal(searchValue);
    closeTable(table);
}

int stringCompare(Value *left, Value *right)
{
    char *leftStr = left->v.stringV;
    char *rightStr = right->v.stringV;

    // Trim trailing spaces from both strings
    int leftLen = strlen(leftStr);
    int rightLen = strlen(rightStr);

    while (leftLen > 0 && leftStr[leftLen - 1] == ' ')
        leftLen--;
    while (rightLen > 0 && rightStr[rightLen - 1] == ' ')
        rightLen--;

    return strncmp(leftStr, rightStr, (leftLen < rightLen) ? leftLen : rightLen);
}

void executeScanInterface(RM_TableData *table)
{
    if (tableName[0] == '\0')
    {
        char useExisting;
        printf("No table currently selected. Do you want to use an existing table? (y/n): ");
        scanf(" %c", &useExisting);

        if (useExisting == 'y' || useExisting == 'Y')
        {
            printf("Enter the name of the existing table: ");
            scanf("%s", tableName);
        }
        else if (useExisting == 'n' || useExisting == 'N')
        {
            printf("Error: Please create a table first.\n");
            return;
        }
        else
        {
            printf("Invalid input. Please enter y or n.\n");
            return;
        }
    }

    RC rc = openTable(table, tableName);
    if (rc != RC_OK)
    {
        printf("Error: Unable to open table '%s'. Error code: %d\n", tableName, rc);
        return;
    }

    RM_ScanHandle scanHandle;
    Expr *condition = NULL;
    Expr *left = NULL;
    Expr *right = NULL;

    int attrNum;
    int opType;
    Value *val = (Value *)malloc(sizeof(Value));

    if (val == NULL)
    {
        printf("Memory allocation failed for Value!\n");
        closeTable(table);
        return;
    }

    // Display available attributes
    printf("Available attributes:\n");
    for (int i = 0; i < table->schema->numAttr; i++)
    {
        printf("%d: %s\n", i, table->schema->attrNames[i]);
    }

    // Prompt user for attribute number
    printf("Enter attribute number for condition: ");
    if (scanf("%d", &attrNum) != 1)
    {
        printf("Error reading attribute number.\n");
        free(val);
        closeTable(table);
        return;
    }

    // Validate attribute number
    if (attrNum < 0 || attrNum >= table->schema->numAttr)
    {
        printf("Invalid attribute number!\n");
        free(val);
        closeTable(table);
        return;
    }

    val->dt = table->schema->dataTypes[attrNum];

    switch (val->dt)
    {
    case DT_INT:
        printf("Enter integer value for condition: ");
        scanf("%d", &val->v.intV);
        break;
    case DT_FLOAT:
        printf("Enter float value for condition: ");
        scanf("%f", &val->v.floatV);
        break;
    case DT_BOOL:
        printf("Enter boolean value for condition (0 for false, 1 for true): ");
        int boolInput;
        scanf("%d", &boolInput);
        val->v.boolV = (boolInput != 0);
        break;
    case DT_STRING:
        val->v.stringV = (char *)malloc(table->schema->typeLength[attrNum] + 1);
        if (val->v.stringV == NULL)
        {
            printf("Memory allocation failed for string value!\n");
            free(val);
            return;
        }
        printf("Enter string value for condition: ");
        scanf("%s", val->v.stringV);
        // Pad the input string to match the attribute length
        int padLength = table->schema->typeLength[attrNum] - strlen(val->v.stringV);
        if (padLength > 0)
        {
            memset(val->v.stringV + strlen(val->v.stringV), ' ', padLength);
            val->v.stringV[table->schema->typeLength[attrNum]] = '\0';
        }
        break;
    }

    // Prompt user for operation type
    printf("Select operation type from following options:\n");
    printf("0: AND\n1: OR\n2: NOT\n3: EQUAL\n4: SMALLER\n");
    printf("Enter operation type: ");
    scanf("%d", &opType);

    // Create the expression tree
    MAKE_CONS(left, val);
    MAKE_ATTRREF(right, attrNum);

    // Handle the selected operation type
    switch ((OpType)opType)
    {
    case OP_BOOL_AND:
    case OP_BOOL_OR:
    {
        if ((OpType)opType == OP_BOOL_AND)
        {
            printf("CAUTION - AND operator is for complex records so make sure that the record has enough attributes!\n");
        }

        // For AND and OR, we need two conditions
        Expr *leftCond, *rightCond;

        // First condition
        if (val->dt == DT_STRING)
        {
            MAKE_BINOP_EXPR(leftCond, right, left, OP_COMP_EQUAL);
            leftCond->expr.op->type = OP_COMP_EQUAL;
        }
        else
        {
            MAKE_BINOP_EXPR(leftCond, right, left, OP_COMP_EQUAL);
        }

        // Second condition (allow user to choose a different attribute)
        int attrNum2;
        Value *val2 = (Value *)malloc(sizeof(Value));

        printf("Enter attribute number for second condition (0 to 4): ");
        scanf("%d", &attrNum2);

        if (attrNum2 < 0 || attrNum2 >= table->schema->numAttr)
        {
            printf("Invalid attribute number!\n");
            free(val);
            free(val2);
            closeTable(table);
            return;
        }

        val2->dt = table->schema->dataTypes[attrNum2];
        printf("Enter value for second condition: ");
        switch (val2->dt)
        {
        case DT_INT:
            scanf("%d", &val2->v.intV);
            break;
        case DT_FLOAT:
            scanf("%f", &val2->v.floatV);
            break;
        case DT_BOOL:
            scanf("%hd", &val2->v.boolV);
            break;
        case DT_STRING:
            val2->v.stringV = (char *)malloc(table->schema->typeLength[attrNum2] + 1);
            scanf("%s", val2->v.stringV);
            // Pad the input string to match the attribute length
            int padLength = table->schema->typeLength[attrNum2] - strlen(val2->v.stringV);
            if (padLength > 0)
            {
                memset(val2->v.stringV + strlen(val2->v.stringV), ' ', padLength);
                val2->v.stringV[table->schema->typeLength[attrNum2]] = '\0';
            }
            break;
        }

        Expr *left2, *right2;
        MAKE_CONS(left2, val2);
        MAKE_ATTRREF(right2, attrNum2);

        if (val2->dt == DT_STRING)
        {
            MAKE_BINOP_EXPR(rightCond, right2, left2, OP_COMP_EQUAL);
            rightCond->expr.op->type = OP_COMP_EQUAL;
        }
        else
        {
            MAKE_BINOP_EXPR(rightCond, right2, left2, OP_COMP_EQUAL);
        }

        // Combine the two conditions
        MAKE_BINOP_EXPR(condition, leftCond, rightCond, (OpType)opType);
    }
    break;
    case OP_BOOL_NOT:
    {
        Expr *innerCond;
        if (val->dt == DT_STRING)
        {
            MAKE_BINOP_EXPR(innerCond, right, left, OP_COMP_EQUAL);
            innerCond->expr.op->type = OP_COMP_EQUAL;
        }
        else
        {
            MAKE_BINOP_EXPR(innerCond, right, left, OP_COMP_EQUAL);
        }
        MAKE_UNOP_EXPR(condition, innerCond, OP_BOOL_NOT);
    }
    break;
    case OP_COMP_EQUAL:
        printf("Selected EQUAL operation.\n");
        if (val->dt == DT_STRING)
        {
            MAKE_BINOP_EXPR(condition, right, left, OP_COMP_EQUAL);
            condition->expr.op->type = OP_COMP_EQUAL;
        }
        else
        {
            MAKE_BINOP_EXPR(condition, right, left, OP_COMP_EQUAL);
        }
        break;
    case OP_COMP_SMALLER:
        printf("Selected SMALLER operation.\n");
        if (val->dt == DT_STRING)
        {
            printf("SMALLER operation not supported for strings.\n");
            free(val);
            closeTable(table);
            return;
        }
        else
        {
            MAKE_BINOP_EXPR(condition, right, left, OP_COMP_SMALLER);
        }
        break;

    default:
        printf("Invalid operation type selected!\n");
        free(val);
        closeTable(table);
        return;
    }

    // Start the scan with the constructed condition
    rc = startScan(table, &scanHandle, condition);
    if (rc != RC_OK)
    {
        printf("Error starting scan! RC: %d\n", rc);
        free(val);
        closeTable(table);
        return;
    }

    Record *record = (Record *)malloc(sizeof(Record));
    if (record == NULL)
    {
        printf("Memory allocation failed for Record!\n");
        free(val);
        if (val->dt == DT_STRING)
            free(val->v.stringV);
        return;
    }
    rc = createRecord(&record, table->schema);
    if (rc != RC_OK)
    {
        printf("Error creating record!\n");
        free(val);
        if (val->dt == DT_STRING)
            free(val->v.stringV);
        return;
    }

    bool recordsFound = false;
    while (next(&scanHandle, record) != RC_RM_NO_MORE_TUPLES)
    {
        // Check if the record is valid (not deleted)
        if (record->data[0] == '\0') // Assuming '\0' indicates a deleted record
        {
            continue; // Skip this record
        }
        printf("Retrieved record at page %d slot %d\n", record->id.page, record->id.slot);
        recordsFound = true;

        char *recordData = strdup(record->data);
        if (recordData == NULL)
        {
            printf("Error: Unable to allocate memory for record data.\n");
            continue;
        }

        char *token;
        char *rest = recordData;

        for (int i = 0; i < table->schema->numAttr; i++)
        {
            if (i == 0)
            {
                token = strtok_r(rest, ",", &rest);
                if (token != NULL && token[0] == '|')
                {
                    token++; // Skip the '|'
                }
            }
            else
            {
                token = strtok_r(NULL, ",", &rest);
            }

            if (token == NULL)
            {
                printf("Error parsing record data for attribute %d\n", i);
                break;
            }

            printf("%s: ", table->schema->attrNames[i]);

            switch (table->schema->dataTypes[i])
            {
            case DT_INT:
                printf("%d", atoi(token));
                break;
            case DT_FLOAT:
                printf("%f", atof(token));
                break;
            case DT_BOOL:
                printf("%s", strcmp(token, "0") == 0 ? "false" : "true");
                break;
            case DT_STRING:
                printf("%s", token);
                break;
            default:
                printf("Unknown data type");
            }
            printf("\n");
        }
        free(recordData);
        printf("\n"); // Add a newline between records
    }

    if (!recordsFound)
    {
        printf("No records found matching the condition.\n");
    }
    closeScan(&scanHandle);
    freeRecord(record);
    free(val);
    closeTable(table);
}

void showAllRecordsInterface(RM_TableData *table)
{
    if (tableName[0] == '\0')
    {
        char useExisting;
        printf("No table currently selected. Do you want to use an existing table? (y/n): ");
        scanf(" %c", &useExisting);
        if (useExisting == 'y' || useExisting == 'Y')
        {
            printf("Enter the name of the existing table: ");
            scanf("%s", tableName);
        }
        else
        {
            printf("Error: Please create a table first.\n");
            return;
        }
    }

    RC rc = openTable(table, tableName);
    if (rc != RC_OK)
    {
        printf("Error: Unable to open table '%s'. Error code: %d\n", tableName, rc);
        return;
    }

    RM_ScanHandle *scanHandle = (RM_ScanHandle *)malloc(sizeof(RM_ScanHandle));
    if (scanHandle == NULL)
    {
        printf("Memory allocation failed for ScanHandle!\n");
        closeTable(table);
        return;
    }

    // Create a condition that's always true
    Expr *condition = (Expr *)malloc(sizeof(Expr));
    condition->type = EXPR_CONST;
    condition->expr.cons = (Value *)malloc(sizeof(Value));
    condition->expr.cons->dt = DT_BOOL;
    condition->expr.cons->v.boolV = true;

    // Start a scan with the always-true condition
    rc = startScan(table, scanHandle, condition);
    if (rc != RC_OK)
    {
        printf("Error starting scan! RC: %d\n", rc);
        free(condition->expr.cons);
        free(condition);
        free(scanHandle);
        closeTable(table);
        return;
    }

    Record *record = (Record *)malloc(sizeof(Record));
    if (record == NULL)
    {
        printf("Memory allocation failed for Record!\n");
        free(condition->expr.cons);
        free(condition);
        closeScan(scanHandle);
        free(scanHandle);
        closeTable(table);
        return;
    }

    rc = createRecord(&record, table->schema);
    if (rc != RC_OK)
    {
        printf("Error creating record! RC: %d\n", rc);
        free(record);
        free(condition->expr.cons);
        free(condition);
        closeScan(scanHandle);
        free(scanHandle);
        closeTable(table);
        return;
    }

    int recordCount = 0;
    while ((rc = next(scanHandle, record)) == RC_OK)
    {
        // Check if the record is valid (not deleted)
        if (record->data[0] == '\0') // Assuming '\0' indicates a deleted record
        {
            continue; // Skip this record
        }
        recordCount++;
        printf("Record %d:\n", recordCount);

        char *recordData = strdup(record->data);
        if (recordData == NULL)
        {
            printf("Error: Unable to allocate memory for record data.\n");
            continue;
        }

        char *token;
        char *rest = recordData;

        for (int i = 0; i < table->schema->numAttr; i++)
        {
            if (i == 0)
            {
                token = strtok_r(rest, ",", &rest);
                if (token != NULL && token[0] == '|')
                {
                    token++; // Skip the '|'
                }
            }
            else
            {
                token = strtok_r(NULL, ",", &rest);
            }

            if (token == NULL)
            {
                printf("Error parsing record data for attribute %d\n", i);
                break;
            }

            printf("%s: ", table->schema->attrNames[i]);

            switch (table->schema->dataTypes[i])
            {
            case DT_INT:
                printf("%d", atoi(token));
                break;
            case DT_FLOAT:
                printf("%f", atof(token));
                break;
            case DT_BOOL:
                printf("%s", strcmp(token, "0") == 0 ? "false" : "true");
                break;
            case DT_STRING:
                printf("%s", token);
                break;
            default:
                printf("Unknown data type");
            }
            printf("\n");
        }
        printf("\n");
        free(recordData);
    }

    if (rc != RC_RM_NO_MORE_TUPLES)
    {
        printf("Error during scan. RC: %d\n", rc);
    }

    if (recordCount == 0)
    {
        printf("No records found in the table.\n");
    }
    else
    {
        printf("Total records: %d\n", recordCount);
    }

    freeRecord(record);
    free(condition->expr.cons);
    free(condition);
    closeScan(scanHandle);
    free(scanHandle);
    closeTable(table);
}

void interactiveInterface(RM_TableData *table)
{
    int choice;
    while (1)
    {
        printf("Menu:\n");
        printf("1. Create Table\n");
        printf("2. Insert Record\n");
        printf("3. Update Record\n");
        printf("4. Delete Record\n");
        printf("5. Execute Scan\n");
        printf("6. Show All Records\n");
        printf("7. Exit\n");
        printf("Enter your choice: ");

        // Check if scanf successfully reads an integer
        if (scanf("%d", &choice) != 1)
        {
            // Clear the input buffer
            while (getchar() != '\n')
                ;
            printf("Invalid choice! Please enter a number between 1 and 7.\n");
            continue;
        }

        switch (choice)
        {
        case 1:
            createTableInterface();
            break;
        case 2:
            insertRecordInterface(table);
            break;
        case 3:
            updateRecordInterface(table);
            break;
        case 4:
            deleteRecordInterface(table);
            break;
        case 5:
            executeScanInterface(table);
            break;
        case 6:
            showAllRecordsInterface(table);
            break;
        case 7:
            return;
        default:
            printf("Invalid choice!\n");
        }
    }
}
