                                                        Advanced Database Organization - Fall 2024
                                                                CS 525 - Group 39
                                                        Programming Assignment 4: B+ -Tree Manager
Authors
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
                        Group - 39 
Sundar Machani (smachani@hawk.iit.edu) - A20554747
Dhruvi Pancholi (dpancholi1@hawk.iit.edu) - A20545574
Ruthwik Dhaipulle (rdhaipulle@hawk.iit.edu) - A20548196

Compilation and Execution
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
To compile the project, use the following commands:
* make - compiles all test files (test_assign4_1 and test_assign4_2)
* ./test_assign4_1 - runs the main B-tree tests

######################################## BONUS #######################################################
* ./test_assign4_2 - runs the data type specific tests

* make clean - removes all compiled files and temporary files

Overview
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
This project implements a B-tree index manager in C, designed to provide efficient indexing capabilities for a database system.

Features
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
* B-tree creation, insertion, deletion, and search operations
* Tree scanning capabilities
* Integration with buffer manager for efficient I/O operations
######################################## BONUS #######################################################
* Support for multiple data types (INT, FLOAT, STRING, BOOL)

Key Components
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
1. IndexTree Structure:
   - Manages key-value pairs and tree metadata
   - Handles different data types
   - Maintains node relationships

2. Core Operations:
   - Tree creation and deletion
   - Key insertion and deletion
   - Key search functionality
   - Sequential scanning

######################################## BONUS #######################################################
3. Data Type Support:
   - Integer keys
   - Float keys
   - String keys
   - Boolean keys

File Structure:
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
record_mgr.h: Header file containing the interface for the record manager.
record_mgr.c: Source file implementing the record manager functions.
rm_serializer.c: Source file for serialization and deserialization of records and schemas.
buffer_mgr.h: Header file for the buffer manager interface.
buffer_mgr.c: Implementation of the buffer manager.
storage_mgr.h: Header file for the storage manager interface.
storage_mgr.c: Implementation of the storage manager.
dberror.h: Header file defining error codes and constants.
dberror.c: Source file implementing error handling functions.
test_helper.h: Header file with test macros and helper functions.
makefile: Steps for compilation and execution.
btree_mgr.h: Header file containing the B-tree manager interface
btree_mgr.c: Implementation of B-tree operations
test_assign4_1.c: Main test cases for B-tree functionality
test_assign4_2.c: Data type specific test cases
README.txt: This file.

Implementation Details
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
The B-tree implementation includes:
* Dynamic node management
* Multi-data type support
* Efficient key comparison operations
* Buffer management integration
* Memory management for different data types
* Tree traversal and scanning capabilities

Testing
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
Two test suites are provided:
1. test_assign4_1.c:
   - Basic B-tree operations
   - Insertion and deletion
   - Tree scanning

2. test_assign4_2.c:
   - Data type specific tests
   - Type comparison operations
   - Mixed data type handling

Note: This implementation builds upon previous assignments, integrating B-tree indexing capabilities with efficient buffer handling for database operations.