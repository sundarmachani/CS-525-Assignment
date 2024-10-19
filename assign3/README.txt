# Assignment - 3

## Record Manager

#### HOW TO EXECUTE THE SCRIPT

1. Open terminal 

2. Run the command: "make"

3. Run the command:"./test_assign3" (For MAC and Linux), "test_assign3" (For Windows)

4. Run the command: "make test_expr"

4. Run the command: "./test_expr" (For MAC and Linux), "test_expr" (For Windows)

4. To remove object files run the command "make clean"

Note: Change rm to del for make clean in make file for windows. 

## Overview of the Assignment:


### Record Manager Functions:
1. initRecordManager():
    - Initializes the record manager by calling the initStorageManager function.
    - Returns RC_OK if the initialization is successful.
2. shutdownRecordManager():
    - Clears the record manager pointer and frees associated memory.
    - Returns RC_OK if the record manager is successfully shut down.
3. createTable():
    - Creates a new table with the given name and schema.
    - Initializes buffer pool for the table and writes page data to the first page of the file.
4. openTable():
    - Opens an existing table and initializes its metadata.
    - Pins the first page of the table to read its metadata and allocates memory for the schema.
5. closeTable():
    - Closes the table identified by the given table handle, releasing any resources associated with it.
    - Ensures that all dirty pages related to the table are written to disk before closing.
    - Returns RC_OK if the table is successfully closed and all resources are released.
6. deleteTable():
    - Deletes the table and all its associated data from the database.
    - Removes the table's metadata and frees up any resources occupied by the table.
    - Ensures that any indexes, constraints, or triggers associated with the table are also removed.
    - 
