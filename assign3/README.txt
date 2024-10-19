                                                        Advanced Database Organization - Fall 2024
                                                                CS 525 - Group 39
                                                        Programming Assignment 3: Record Manager
Authors
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
                        Group - 39 
Sundar Machani (smachani@hawk.iit.edu) - A20554747
Dhruvi Pancholi (dpancholi1@hawk.iit.edu) - A20545574
Ruthwik Dhaipulle (rdhaipulle@hawk.iit.edu) - A20548196

Compilation and Execution
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
To compile the project, use the following command:
* to compile the test_assign3_1 and InteractiveInterface use : make
* to run the test_assign3_1 use : make test3

*************************************** BONUS (InteractiveInterface) ************************************************
* to intiate the Interactive Interface window use : make ii
* to clean or remove compiled file both test3 and InteractiveInterface use : make clean

Overview
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
This project implements a Record Manager in C, designed to efficiently manage records in a database system.

Features
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
* Record creation, insertion, updating, and deletion
* Table and schema management
* Scan operations for record retrieval
* Integration with buffer manager for efficient I/O operations

* Includes InteractiveInterface which uses "command line Interface" for interacting.
* Has opertaions for following:

1. Create Table
2. Insert Record
3. Update Record
4. Delete Record
5. Execute Scan
6. Show All Records
7. Exit

Structure 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
The project consists of the following main components:

* Record Manager: Provides APIs for managing records and tables
* Table and Schema Management: Handles table creation, schema definition, and metadata
* Scan Operations: Implements functionality for scanning and retrieving records based on conditions

Key Data Structures:

1. ScanData: A struct used to manage scan operations on records. It contains the following fields:
   - thisPage: Current page being scanned
   - thisSlot: Current slot being scanned
   - numOfPages: Total number of pages in the table
   - totalNumSlots: Total number of slots in a page
   - theCondition: Pointer to the condition expression for the scan

   This structure is crucial for maintaining the state of a scan operation, allowing for efficient traversal of records and application of scan conditions.

2. Record: Represents a single record in the database
3. Schema: Defines the structure of a table, including attribute names, types, and sizes
4. RM_TableData: Contains metadata about a table, including its name, schema, and management data

The ScanData struct, in particular, enables the implementation of efficient scanning operations, supporting features like conditional record retrieval and iterative processing of table contents.

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
test_assign3_1.c: Source file containing test cases for record manager functions.
makefile: Steps for compilation and execution.
README.txt: This file.

Note: This implementation builds upon the previous buffer manager assignment, integrating record management capabilities with efficient buffer handling for database operations.