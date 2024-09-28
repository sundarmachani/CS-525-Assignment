                                                        Advanced Database Organization - Fall 2024
                                                                CS 525 - Group 39
                                                        Programming Assignment 2: buffer Manager
Authors
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
                        Group - 39 
Sundar Machani (smachani@hawk.iit.edu) - A20554747
Dhruvi Pancholi (dpancholi1@hawk.iit.edu) - A20545574
Ruthwik Dhaipulle (rdhaipulle@hawk.iit.edu) - A20548196

Compilation and Execution
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
To compile the project, use the following command:
1. to compile the test_assign2_1 use (test_assign2_1 is defaulted to run) : make
2. to compile the test_assign2_1 manually use : make test1
3. to see the output after running the test_assign2_1 use : make run_test1
4. to compile the test_assign2_2 use : make test2        * Code for this is not implemented so expect failure*
5. to see the output after running the test_assign2_1 use : make run_test2
6. to clean or remove compiled file both test1 and test2 use : make clean

Overview
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
A buffer pool manager implementation in C, designed to efficiently manage a pool of buffers for disk I/O operations.

Features
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
*   Buffer pool initialization and management
*   Buffer allocation and deallocation
*   Read and write operations on buffers
*   Support for disk I/O operations

Structure 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
The project consists of the following components:

*   BPData: A struct that represents the buffer pool data, including the buffer pool itself, linked list pointers, and operation counters.
*   BufferPoolFrame: A struct that represents a single buffer pool frame, containing a pointer to the buffer data and other metadata.
*   freeBufferPoolData: A function that frees the buffer pool data and resets the pointers and counters.

File Structure:
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
buffer_mgr.h: Header file containing the interface for the buffer manager.
buffer_mgr.c: Source file implementing the buffer manager functions.
dberror.h: Header file defining error codes and constants.
dberror.c: Source file implementing error handling functions.
test_helper.h: Header file with test macros and helper functions.
test_assign2_1.c: Source file containing test cases for the buffer manager functions.
test_assign2_2.c: Source file containing test cases for the RS_LRU_K manager functions.  * Not implemented *
makefile: steps for compilation and Execution.
README.txt: This file.