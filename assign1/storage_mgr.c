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
    /*      open the file for read and write based on fileName and if file is found then truncate it's contents,
        if not found create it. */
    FILE *filePointer = fopen(fileName, "w+");
    if (filePointer == NULL)
    {
        // return error if file creation or open file fails.
        return RC_FILE_NOT_FOUND;
    }

    // Allocating memory for one page and setting it to '\0' (by default calloc intializes to zero).
    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    // return error in case of emptyPage creation fails.
    if (emptyPage == NULL)
    {
        fclose(filePointer); // closing the file before leaving inorder to avoid memory leakage.
        return RC_WRITE_FAILED;
    }

    // adding empty page created earlier (emptyPage) to the file.
    size_t writeResult = fwrite(emptyPage, sizeof(char), PAGE_SIZE, filePointer);
    if (writeResult < PAGE_SIZE)
    {
        free(emptyPage);     // Free allocated memory.
        fclose(filePointer); // Close the file to prevent leakage.
        return RC_WRITE_FAILED;
    }

    // Clean up and close the file.
    free(emptyPage);
    fclose(filePointer);

    return RC_OK;
}

// Open the file page if the file is found and intialize the 'SM_FileHandle'.
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    // Opening the file with read and write ability based on fileName if not found return ERROR.
    FILE *filePointer = fopen(fileName, "r+");
    if (filePointer == NULL)
    {
        // return error if file not found
        return RC_FILE_NOT_FOUND;
    }

    // Initialize the file handle structure.
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;

    // Finding total number of pages in the file.
    /* Move filePointer to the end of the file using fseek() which basically has ability to traverse through the file.
       SEEK_END specifies to move to the end of the file or last page. */
    fseek(filePointer, 0, SEEK_END);
    /* Get the file size as we already movef to the end of the file before using fseek(),
     ftell() returns current page position in this case last page. */
    long fileSize = ftell(filePointer);
    // get the totalNumPages by fileSize / 4096;
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

    // creating a local instance of filePointer from SM_FileHandle structure.
    FILE *filePointer = (FILE *)fHandle->mgmtInfo;
    // Close the file
    fclose(filePointer);

    // making mgntInfo NULL to indicate that file is closed
    fHandle->mgmtInfo = NULL;

    return RC_OK;
}

// deletes a file from the disk
RC destroyPageFile(char *fileName)
{
    // Delete the file with the given fileName using remove() which return 0 on succesful deletion.
    if (remove(fileName) != 0)
    {
        // Return file not found error if the file does not exist
        return RC_FILE_NOT_FOUND;
    }

    return RC_OK;
}

// -------------------------------------  READ BLOCKS FROM DISC  ------------------------------------------------

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if the page number is valid
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE; // Return non existing page error
    }

    // find correct page 
    if (fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET) != 0)
    {
        return RC_FILE_HANDLE_NOT_INIT; // seek failed error
    }

    // Read  page into mem
    size_t readResult = fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    if (readResult < PAGE_SIZE)
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error  read fail 
   }

    // Update  current page position
    fHandle->curPagePos = pageNum;

    return RC_OK; // Return success
}


int getBlockPos(SM_FileHandle *fHandle)
{
    // Return current page pos
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Read first block
    return readBlock(0, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Read last block 
    int minusOne = fHandle->totalNumPages - 1;
    return readBlock(minusOne, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check for previous block
    if (fHandle->curPagePos <= 0)
        return RC_READ_NON_EXISTING_PAGE; // no previous page

    // Read previous block, update curPagePos
    int thePrevious = fHandle->curPagePos - 1;
    return readBlock(thePrevious, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Read current block
    int curBlock = fHandle->curPagePos;
    return readBlock(curBlock, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check next block
    int checkNext = fHandle-> totalNumPages - 1;
    if (fHandle->curPagePos >= checkNext)
        return RC_READ_NON_EXISTING_PAGE; // No next page 

    // Read the next block, update curPagePos
    int readNext = fHandle->curPagePos+1;
    return readBlock(readNext, fHandle, memPage);
}


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
