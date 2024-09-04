                                    Advanced Database Organization - Fall 2024
                                                CS 525 - Group 39
                                        Programming Assignment I: Storage Manager
Authors
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
                        Group - 39 
Sundar Machani (smachani@hawk.iit.edu) - A20554747



Overview
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
This project implements a simple storage manager module in C. 
The storage manager is designed to handle file operations for a system that deals with pages (blocks) of fixed size. 
It includes functionalities for creating, opening, closing, and destroying page files, as well as reading and writing pages from/to these files.

Features
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
File Handling Methods:
createPageFile: Creates a new page file with an initial size of one page, filled with zero bytes. 
                Handled error - if "RC_FILE_NOT_FOUND: not able to create file","RC_WRITE_FAILED: if not able to create an empty page",
                                "RC_WRITE_FAILED: if not able to add the created empty page to the file"
openPageFile: Opens an existing page file and initializes a file handle with information about the file, 
              Handled errors - "RC_FILE_NOT_FOUND: if file not found",
              initialize - fHandle->fileName, fHandle->curPagePos, fHandle->totalNumPages, fHandle->mgmtInfo
closePageFile: Closes an open page file,
               Handled error - "RC_FILE_HANDLE_NOT_INIT: if fHandle->mgmtInfo is NULL".
               initialize - fHandle->mgmtInfo to NULL before exiting this function;
destroyPageFile: Deletes a page file from the file system.
                Handled error - "RC_FILE_NOT_FOUND : if fileName is not found"

Page Handling Methods:
readBlock: Reads a specified block from the file into memory.
writeBlock: Writes a block of memory to a specified location in the file.
readFirstBlock, readPreviousBlock, readCurrentBlock, readNextBlock, readLastBlock: Methods for reading specific blocks based on position.
writeCurrentBlock: Writes the current block in memory to the file.
appendEmptyBlock: Appends an empty block to the end of the file.
ensureCapacity: Ensures that the file has enough pages to accommodate the specified number.

File Structure:
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
storage_mgr.h: Header file containing the interface for the storage manager.
storage_mgr.c: Source file implementing the storage manager functions.
dberror.h: Header file defining error codes and constants.
dberror.c: Source file implementing error handling functions.
test_helper.h: Header file with test macros and helper functions.
test_assign1_1.c: Source file containing test cases for the storage manager functions.
makefile: steps for compilation and Execution.
README.txt: This file.

Compilation and Execution
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
To compile the project, use the following command:
1. to compile and run test cases : makefile
2. to clean or remove compiled file: make clean
