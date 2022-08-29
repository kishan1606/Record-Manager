#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *SF;

int file_close_checks(FILE *filename){
	int file_code = fclose(filename);
	if(file_code == 0){
		return RC_OK;
	}else{
		return RC_FILE_CLOSING_ERROR;
	}
}

int file_open_checks(char *filename, char operation[]){
	SF = fopen(filename, operation);
	if(SF != NULL){
		// File Found
		// Check for handler issues if non 0 meaning OK
		if(SF != 0){
			return RC_OK;
		}else{
			// File has no Handler which means File init failed
			return RC_FILE_HANDLE_NOT_INIT;
		}
	}else{
		// File Not Found ERROR Returned
		return RC_FILE_NOT_FOUND;
	}
}

extern void initStorageManager (void) {
	// Initialising file pointer i.e. storage manager.
	SF = NULL;
}

extern RC createPageFile (char *fileName) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	// Open/Create a file if already there/not present respectively
	SF = fopen(fileName, "w+");
	if(SF != 0){
		// Everything is OK
		// Used malloc for custom set of variable but it is almost similar to calloc
		char *mBlock = malloc(PAGE_SIZE * sizeof(char));
		memset(mBlock, '\0', PAGE_SIZE); // Set all the bytes to '\0'
		fwrite(mBlock, sizeof(char), PAGE_SIZE, SF);
		return_code = file_close_checks(SF); // Performing Close File Checks
	}else{
		// Incase File is not opened meaning file handler error which is already defined so return that
		return_code = RC_FILE_HANDLE_NOT_INIT; 
	}

	return return_code;
}


extern RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = file_open_checks(fileName, "r");

	if(return_code == RC_OK){
		// File Open Successful
        (*fHandle).fileName = fileName;				// Update file handle's filename to our filename and set the current page position to 0 i.e. the start of the page.
        (*fHandle).curPagePos = 0;
		(*fHandle).mgmtInfo = SF;
        
        struct stat Info;				// Initialize a structure
   
        if(fstat(fileno(SF), &Info) < 0)				//Use fstat() to get the file total size.used fileno(file) function to returns the file descriptor of the specified file and getting the information of File and storing it in info.
        	return_code = RC_ERROR;
		else{
        	(*fHandle).totalNumPages = Info.st_size/ PAGE_SIZE;				//setting the totla page number of file to totalNumPages Parameter
        	fclose(SF);				// close file stream and flush all the buffers.
        	return_code = RC_OK;
		}
	}else{
		return_code = RC_FILE_NOT_FOUND;
	}
	return return_code;
}

extern RC closePageFile (SM_FileHandle *fHandle) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	
	// To Perform close checks we need to open the file again
	return_code = file_open_checks((*fHandle).fileName, "r");

	// Perform File Closing Checks using the custom function
	if(return_code == RC_OK)
		return_code = file_close_checks(SF);

	return return_code;
}


extern RC destroyPageFile (char *fileName) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// Will check read file using basic "r" as we just check if file exist to destory
	// "r" makes sure that new file is not created and that we get ERROR that there is no file to destroy
	return_code = file_open_checks(fileName, "r");

	if(return_code != RC_OK)
		return return_code;
	else{
		// Close the file with checks if RC_OK to avoid memory leaks
		return_code = file_close_checks(SF);
		if(return_code == RC_OK){
			// As all checks passed we now try to destroy the file so that its no longer accessible		
			int destroy_code = remove(fileName);
			if(destroy_code == 0)
				return_code = RC_OK;
			else
				return_code = RC_OK;
		}
	}

	return return_code;
}

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code, size_of_read;

	// Open the file mentioned in the File Handler using custom checks
	return_code = file_open_checks((*fHandle).fileName, "r");
	
	if(return_code != RC_OK)
		// File Open checks failed
		// Return now itself so as file close checks are not performed
		return return_code;
	else{
		// All Checks returned RC_OK
		// Check if the page number is less than Total Pages and also less than 0
		if (pageNum > (*fHandle).totalNumPages || pageNum < 0)
			// Returns NON_EXISTING_PAGE since the page doesn't exit to read
        	return_code = RC_READ_NON_EXISTING_PAGE;
		else{
			// Page exist to read
			fseek(SF, (pageNum * PAGE_SIZE), SEEK_SET);				// seek will be success if fseek() will return 0. We have to Set the cursor pointer position to the page we want to read and this position is calculated by multiplying Page Number to the Page Size.
			size_of_read=fread(memPage, sizeof(char), PAGE_SIZE, SF);				//fread() will return the size of read memory
			if(size_of_read < PAGE_SIZE || size_of_read > PAGE_SIZE)			// check if the size of read is greater then or less then the actual page size, if not then throwing an error
				return_code = RC_READ_NON_EXISTING_PAGE;
			else
				// As everything is OK
				return_code = RC_OK;
		}
	}

	// If until now everything is OK then only perform file close checks
	// Else return the return_code
	if(return_code == RC_OK)
		return_code = file_close_checks(SF);

	return return_code;
}

extern int getBlockPos (SM_FileHandle *fHandle) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code, curPos;

	// Check if there is any filehandler
	if(fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	else{
		// File Handler Exists
		// Check if the file exist
		// If it does then only return PagePos so as it is useful
		// If not then its no point of CurPagePos
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			// File openinf was successful
			// Return cur Poss
			if((*fHandle).mgmtInfo != NULL){
				curPos = (*fHandle).curPagePos;
			}
			else
			 	return_code = RC_FILE_MGMT_INFO_ERROR; 
		}
	}

	// Close the file irrespective of return_code/error_code to avoid memory leaks
	file_close_checks(SF);
	if(return_code == RC_OK)
		return curPos;
	else
		return return_code;
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	if(fHandle == NULL)
		// File Handler doesn't exist
		return RC_FILE_HANDLE_NOT_INIT;
	else{
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			if((*fHandle).mgmtInfo != NULL)				// Redirecting to readBlock() function with page number of first block i.e 0.
				return readBlock(0, fHandle, memPage);
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
		else
			return_code = RC_FILE_HANDLE_NOT_INIT;
	}

	file_close_checks(SF);
	return return_code;
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// RC is already defined in dberror.h file using it to store RC_CODES & other values
	RC Output, return_code;

	if(fHandle != NULL){
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			if((*fHandle).mgmtInfo != NULL){
				int prePageNo = 0;				//initialize page number to 0
				if((*fHandle).curPagePos >=1)					//check if current page position is grearter than or equal to 1. if so then set previous page number to current page position - 1;
					prePageNo = ((*fHandle).curPagePos / PAGE_SIZE) - 1;
				else				//return respective error code if there is any error.
					return_code = RC_READ_NON_EXISTING_PAGE;
				
				if(return_code != RC_READ_NON_EXISTING_PAGE){
					Output = readBlock(prePageNo, fHandle, memPage);				// Reading the Block by redirecting to ReadBlock() function with previous page number.
					if(Output==0){
						(*fHandle).curPagePos=prePageNo;
					}
					return Output;
				}
			}
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
		else
			return_code = RC_FILE_HANDLE_NOT_INIT;
	}
	else
		// File Handler doesn't exist
		return RC_FILE_HANDLE_NOT_INIT;

	file_close_checks(SF);
	return return_code;
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// RC is already defined in dberror.h file using it to store RC_CODES & other values
	RC Output, return_code;

	if(fHandle != NULL){
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			if((*fHandle).mgmtInfo != NULL){
				int curPNo = (*fHandle).curPagePos / PAGE_SIZE;				// Calculate the current page number by dividing page size by current page position.
				Output = readBlock(curPNo, fHandle, memPage);				// Reading the Block by redirecting to ReadBlock() function with current page number.
				if(Output==0){
					(*fHandle).curPagePos=curPNo;
				}
				return Output;
			}
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
		else
			return_code = RC_FILE_HANDLE_NOT_INIT;
	}
	else
		// File Handler doesn't exist
		return RC_FILE_HANDLE_NOT_INIT;

	file_close_checks(SF);
	return return_code;

}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	// RC is already defined in dberror.h file using it to store RC_CODES & other values
	RC Output, return_code;

	if(fHandle != NULL){
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			// Check if Management is Not Null
			if((*fHandle).mgmtInfo != NULL){
				
				if (((*fHandle).totalNumPages-1) < (*fHandle).curPagePos)
					return_code = RC_READ_NON_EXISTING_PAGE;
				else{
					// Check if there is a read error
					if(fread(memPage,1,PAGE_SIZE,(*fHandle).mgmtInfo) != PAGE_SIZE)
            			return_code = RC_FILE_READ_ERROR;
					else{
						int nxtPgNo = ((*fHandle).curPagePos / PAGE_SIZE) + 1;				// Calculate the next page number by dividing page size by current page position and adding 1.
						Output = readBlock(nxtPgNo, fHandle, memPage);				// Reading the Block by redirecting to ReadBlock() function with next page number.
						if(Output==0){
							(*fHandle).curPagePos=nxtPgNo;
						}
						return Output;
					}
				}
			}
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
	}
	else
		// File Handler doesn't exist
		return RC_FILE_HANDLE_NOT_INIT;

	file_close_checks(SF);
	return return_code;
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	if(fHandle != NULL){
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r");
		if(return_code == RC_OK){
			// Check if Management is Not Null
			if((*fHandle).mgmtInfo != NULL){
				int lstPgno = (*fHandle).totalNumPages - 1;				// Calculate the last page number by subtracting 1 from total page numbers.
				return readBlock(lstPgno, fHandle, memPage);				// Redirecting to readBlock() function with page number of first block i.e 0.
			}
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
	}
	else
		// File Handler doesn't exist
		return RC_FILE_HANDLE_NOT_INIT;

	file_close_checks(SF);
	return return_code;
}


extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	if(fHandle != NULL){
		// File Handler Exists
		// Check if the file exist
		return_code = file_open_checks((*fHandle).fileName, "r+");
		if(return_code == RC_OK){
			// Check if Management is Not Null
			if((*fHandle).mgmtInfo != NULL){
				if (pageNum < 0 || (*fHandle).totalNumPages < pageNum)				// Check if the page number is greater than Total number of pages and also less than 0, if so then return respective error code
        			return_code = RC_WRITE_FAILED;
				else{
					int firstPtr = PAGE_SIZE * pageNum;
					if(pageNum != 0) {
						(*fHandle).curPagePos = firstPtr;			// Set the current page position to the cursor position of the file stream
						file_close_checks(SF);
						writeCurrentBlock(fHandle, memPage);			// Write content to File stream from memPage

					} else {
						fseek(SF, firstPtr, SEEK_SET);
						int i = 0;
						while(i < PAGE_SIZE)
						{
							if(feof(SF)){
								appendEmptyBlock(fHandle);				//insert empty blocks according to page size
							}
							fputc(memPage[i], SF);
							i++;
						}
						(*fHandle).curPagePos = ftell(SF);
						file_close_checks(SF);
					}
				}
			}
			else
				// Management Info is not there Error Returned
				return_code = RC_FILE_MGMT_INFO_ERROR;
		}
	}
	else
		// File Handler doesn't Exist
		return RC_FILE_HANDLE_NOT_INIT;

	// file_close_checks(SF);
	return return_code;
}


extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;

	return_code = file_open_checks((*fHandle).fileName, "r+");
	if(return_code == RC_OK){
		// Check if Management is Not Null
		if((*fHandle).mgmtInfo != NULL){
			appendEmptyBlock(fHandle);					//insert empty blocks according to page size
			fseek(SF, (*fHandle).curPagePos, SEEK_SET);
			fwrite(memPage, sizeof(char), strlen(memPage), SF);
			(*fHandle).curPagePos = ftell(SF);
			file_close_checks(SF);
			return RC_OK;
		}
		else{
			//Management Info Error
			return RC_FILE_MGMT_INFO_ERROR;
		}
	}
	return return_code;
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle) {

	if(fHandle != NULL){
		SM_PageHandle mBlock = malloc(PAGE_SIZE * sizeof(char));				// Malloc New memory with '\0' , write it & then free it so as we have no memory leaks
		memset(mBlock, '\0', PAGE_SIZE);

		if(fseek(SF, 0, SEEK_END) != 0){
			free(mBlock);				//free the block
			return RC_WRITE_FAILED;
		}
		else{
			fwrite(mBlock, sizeof(char), PAGE_SIZE, SF);
			free(mBlock);					//free the block
			(*fHandle).totalNumPages++;				//increment total page number
			return RC_OK;
		}
	}
	else
		return RC_FILE_HANDLE_NOT_INIT;
	file_close_checks(SF);
	return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	if(fHandle != NULL){
		// File Handler Exist
		// Check if File Exists
		return_code = file_open_checks((*fHandle).fileName, "a");
		if(return_code == RC_OK){
			int page = (*fHandle).totalNumPages;
			if(numberOfPages > page){
				while(page<numberOfPages){
					appendEmptyBlock(fHandle);
					page++;
				}
			}
			return RC_OK;
		}
	}
	else
		// File Handler Error
		return RC_FILE_HANDLE_NOT_INIT;
	
	file_close_checks(SF);
	return return_code;
}