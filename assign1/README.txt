                                                        Advanced Database Organization - Fall 2024
                                                                CS 525 - Group 39
                                                        Programming Assignment I: Storage Manager
Authors
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
                        Group - 39 
Sundar Machani (smachani@hawk.iit.edu) - A20554747
Dhruvi Pancholi (dpancholi1@hawk.iit.edu) - A20545574
Ruthwik Dhaipulle (rdhaipulle@hawk.iit.edu) - A20548196


Overview
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
This project implements a simple storage manager module in C. 
The storage manager is designed to handle file operations for a system that deals with pages (blocks) of fixed size. 
It includes functionalities for creating, opening, closing, and destroying page files, as well as reading and writing pages from/to these files.

Features
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
File Handling Methods:
createPageFile(): Creates a new page file with an initial size of one page, filled with zero bytes. 
                Handled error - if "RC_FILE_NOT_FOUND: not able to create file","RC_WRITE_FAILED: if not able to create an empty page",
                                "RC_WRITE_FAILED: if not able to add the created empty page to the file"
openPageFile(): Opens an existing page file and initializes a file handle with information about the file, 
              Handled errors - "RC_FILE_NOT_FOUND: if file not found",
              initialize - fHandle->fileName, fHandle->curPagePos, fHandle->totalNumPages, fHandle->mgmtInfo
closePageFile(): Closes an open page file,
               Handled error - "RC_FILE_HANDLE_NOT_INIT: if fHandle->mgmtInfo is NULL".
               initialize - fHandle->mgmtInfo to NULL before exiting this function;
destroyPageFile(): Deletes a page file from the file system.
                Handled error - "RC_FILE_NOT_FOUND : if fileName is not found"
Page Handling Methods:
readBlock(): Reads a specified block from the file into memory 
by checking if pageNum is in valid range, 
and if not it returns RC_READ_NON_EXISTING_PAGE.
getBlockPos():
the function returns the current position of the page by returning the value of the curPagePos variable.
readFirstBlock():
the function reads the last block by calling the readBlock function with the 0th page and loads it into memory.
readLastBlock():
It reads the last block in the file. It does this by subtracting 1 from the total number of pages and calling readBlock to load the last page.
readPreviousBlock():
It reads the previous block from the current page's position. If the current page position is 0, it returns RC_READ_NON_EXISTING_PAGE.
readCurrentBlock():
It reads the current block by calling readBlock w/ the current page position.
readNextBlock():
It reads the next block from the current page position and checks if it's the last page. If it's the last, it returns RC_READ_NON_EXISTING_PAGE.
Else, it increments to read the next page.

writeBlock: Writes a block of memory to a specific page number in the file by seeking to the correct file position and writing the block.
            Handled error - (if the pageNumber is valid), (seek to correct page based on page number), (Write the page from memory (memPage)                             to the file) if any of these validation fails results in RC_WRITE_FAILED.
writeCurrentBlock(): Writes a block to the current file position stored in the file handle’s curPagePos.
                     Handled error - (if the currentPagePosition is legit), (Seek to the current block position based on             
                     CurrentPagePosition), 
                     (Write the memory page(memPage) to the current position) if any of these validation fails results in RC_WRITE_FAILED.
appendEmptyBlock(): Appends a zero-filled block at the end of the file, increasing the file size by one page.
                    Handled error - (if Create an empty page with '/0' bytes is not succesful), (if not able to Move to the end of the file),
                                    (if Writing the empty block (all zeros) to the file) any failure in these validations then           
                                    RC_WRITE_FAILED.  
ensureCapacity(): Ensures the file has enough pages by appending empty blocks if necessary.
                  Handled error - Check if the file already has the maximum numberOfPages,  if succesful then RC_OK
                                  check if we are able to Append empty blocks to reach the value of additionalPages, if failure then return                                    ERROR.

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
1. to compile and run test cases : make
2. to clean or remove compiled file: make clean
