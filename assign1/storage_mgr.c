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
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check if the pageNumber is legit
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
    {
        return RC_WRITE_FAILED; // return back error as page cannot be written
    }
    // seek to correct page based on page number
    if (fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET != 0))
    {
        return RC_WRITE_FAILED; // return error as page cannot be written
    }
    // Write the page from memory (memPage) to the file
    size_t writeResult = fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    if (writeResult < PAGE_SIZE)
    {
        return RC_WRITE_FAILED; // Return error if writing fails
    }
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if the currentPagePosition is legit
    if (fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages)
    {
        return RC_WRITE_FAILED; // Return error if current page is invalid
    }

    // Seek to the current block position based on CurrentPagePosition
    if (fseek(fHandle->mgmtInfo, fHandle->curPagePos * PAGE_SIZE, SEEK_SET) != 0)
    {
        return RC_WRITE_FAILED; // Return error if seek fails
    }

    // Write the memory page(memPage) to the current position
    size_t writeResult = fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    if (writeResult < PAGE_SIZE)
    {
        return RC_WRITE_FAILED; // Return error if writing fails
    }

    return RC_OK; // Return success
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    // Create an empty page with '/0' bytes
    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

    if (emptyPage == NULL)
    {
        return RC_WRITE_FAILED; // Return error if page cannot be written due to memory allocation failure
    }

    // Move to the end of the file
    if (fseek(fHandle->mgmtInfo, 0, SEEK_END) != 0)
    {
        free(emptyPage); // Free or erase the memory before returning
        return RC_WRITE_FAILED;
    }

    // Writing the empty block (all zeros) to the file
    size_t writeResult = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    if (writeResult < PAGE_SIZE)
    {
        free(emptyPage);        // Free the memory before returning
        return RC_WRITE_FAILED; // Return error if writing fails
    }

    // Free or erase the allocated empty page after writing
    free(emptyPage);

    // Updating file's metadata: increment the total number of pages
    fHandle->totalNumPages += 1;

    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    // Check if the file already has the maximum numberOfPages
    if (fHandle->totalNumPages >= numberOfPages)
    {
        return RC_OK; // Ok as no need of adding more pages
    }

    // Calculate number of additional pages can be taken
    int additionalPages = numberOfPages - fHandle->totalNumPages;

    // Append empty blocks to reach the value of additionalPages
    for (int i = 0; i < additionalPages; i++)
    {
        RC result = appendEmptyBlock(fHandle);
        if (result != RC_OK)
        {
            return result; // Return error if appending a block fails
        }
    }

    return RC_OK;
}
