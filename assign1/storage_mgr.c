#include "stdio.h"
#include "stdlib.h"
#include "storage_mgr.h"

// ------------------------------------  PAGE FILES MANIPULATION  ------------------------------------------------

void initStorageManager(void)
{
    // No initialization is required for this function as there's nothing to be intialized.
}

//  Create a new file and adding a new page to it with zero bytes.
RC createPageFile(char *fileName)
{
    // try to open the file for writing and if file is not found create it.
    FILE *filePointer = fopen(fileName, "w+");
    if (filePointer == NULL)
    {
        // return error if file creation fails.
        return RC_FILE_NOT_FOUND;
    }

    // Allocating memory for one page and setting it to '\0'.
    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL)
    {
        fclose(filePointer); // closing the file before leaving inorder to avoid memory leakage.
        return RC_WRITE_FAILED;
    }

    // adding empty page to the file.
    size_t writeResult = fwrite(emptyPage, sizeof(char), PAGE_SIZE, filePointer);
    if (writeResult < PAGE_SIZE)
    {
        free(emptyPage);     // Free allocated memory
        fclose(filePointer); // Close the file to prevent leakage
        return RC_WRITE_FAILED;
    }

    // Clean up and close the file
    free(emptyPage);
    fclose(filePointer);

    return RC_OK;
}

// Open the file page if the file is found and intialize the 'SM_FileHandle'
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    // Opening the file with read and write ability
    FILE *filePointer = fopen(fileName, "r+");
    if (filePointer == NULL)
    {
        // return error if file not found
        return RC_FILE_NOT_FOUND;
    }

    // Initialize the file handle structure
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;

    // Finding total number of pages in the file
    fseek(filePointer, 0, SEEK_END);    // Move to the end of the file
    long fileSize = ftell(filePointer); // Get the file size
    fHandle->totalNumPages = fileSize / PAGE_SIZE;

    // Store the FILE pointer in mgmtInfo
    fHandle->mgmtInfo = filePointer;

    return RC_OK;
}

// close an opened file
RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle->mgmtInfo == NULL)
    {
        // return file handle not initialized error
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Close the file
    FILE *filePointer = (FILE *)fHandle->mgmtInfo;
    fclose(filePointer);

    // making mgntInfo NULL to indicate that file is closed
    fHandle->mgmtInfo = NULL;

    return RC_OK;
}

// deletes a file from the disk
RC destroyPageFile(char *fileName)
{
    // Delete the file with the given fileName
    if (remove(fileName) != 0)
    {
        // Return file not found error if the file does not exist
        return RC_FILE_NOT_FOUND;
    }

    return RC_OK;
}

// -------------------------------------  READ BLOCKS FROM DISC  ------------------------------------------------







// -------------------------------------  WRITE BLOCKS TO A PAGE FILE -------------------------------------------