#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int bufsize = 0, lfuptr = 0, wrtct = 0, ridx = 0, fidx =0, hitctr = 0, clkptr = 0;

PageFrame *PF;
SM_FileHandle SMFH, FH;

// Buffer Pool Check Funtion
// Input -> BufferPool
// Output -> One of the RC Code
int buffer_pool_checks(BM_BufferPool *const bm){
	// There is no BM reference return error
	if(bm == NULL){
		return RC_BUFFER_POOL_INIT;
	}else{
		return RC_OK;
	}
}

// Page Frame Check Function
// Input -> PageFrame
// Output -> One of the RC Code
int page_frame_checks(PageFrame *page){
	// If Page is NULL
	if(page == NULL)
		return RC_PAGE_INIT;
	else{
		// If PageFrame not NULL
		return RC_OK;
	}
	return RC_OK;
}

// FrameSetter -> Setter function to set PageFrame Values
// Input -> Destination, Source, Index
// Output -> None
void frameSetter(PageFrame *page,PageFrame *pageFrame, int Index){
	// Perform PageFrame Checks
	if(page_frame_checks(pageFrame) == RC_OK){
		pageFrame[Index].data = (*page).data;
		pageFrame[Index].pageNum = (*page).pageNum;
		pageFrame[Index].IfPageDirty = (*page).IfPageDirty;
		pageFrame[Index].fixedPageCount = (*page).fixedPageCount;
	}
}

// PgWriter -> Check For Modifications on the Page
// Input -> Buffer Pool, PageFrame, Index
// Output -> None
void pgWriter(BM_BufferPool *const bm,PageFrame *pageFrame, int Index){
	// Perform BufferPool Checks
	if(buffer_pool_checks(bm) ==RC_OK){
		// Perform Page Checks
		if(page_frame_checks(pageFrame) == RC_OK){
			// Check for Modifications in Page
			if(pageFrame[Index].IfPageDirty != 1){
				// No Modifications Found
				// printf("Page Not Modified");
			}else{
				openPageFile((*bm).pageFile, &SMFH);
				writeBlock(pageFrame[Index].pageNum, &SMFH, pageFrame[Index].data);
				wrtct++;
			}
		}
	}
}

// FIFO -> FIFO Stratergy Implementation
// Inputs -> BufferPool, PageFrame
// Output -> None
extern void FIFO(BM_BufferPool *const bm, PageFrame *page){
	// Perform BufferPool Checks
	if(buffer_pool_checks(bm) != RC_OK){
		printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
	}else{
		// Buffer Pool Is OK
		// Perform Page Checks
		if(page_frame_checks(page) != RC_OK){
			printf("Page Init Error: %d", RC_PAGE_INIT);
		}else{
			// Both Page & Buffer Pool OK
			// Perform FIFO Operation
			int i = 0;
			PF = (PageFrame *) (*bm).mgmtData;

			int fidx = ridx - bufsize;
			// Loop is used to set fidx to appropriate location
			while(fidx >= bufsize){
				fidx = fidx - bufsize;	
			}

			while(i < bufsize){
				if(PF[fidx].fixedPageCount != 0)
					// As Current Page is used by someone can't change that
					// Move to next page
					fidx++;
				else{
					// Found a Page not used by anyone
					// Check for Page Modifications & then replace
					pgWriter(bm,PF,fidx);
					frameSetter(page,PF,fidx);
					break;
				}
				i++;
			}
			
		}
	}
}

// LRU -> LRU Stratergy Implementation
// Inputs -> BufferPool, PageFrame
// Output -> None
extern void LRU(BM_BufferPool *const bm, PageFrame *page){	
	// Bufferpool & Page Checks
	if(buffer_pool_checks(bm) != RC_OK && page_frame_checks(page) != RC_OK){
		printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
	}else{
		// Both OK
		int i, j, leastHitI, leastHitN;
		i = j = leastHitI = leastHitN = 0;
		PF = (PageFrame *) (*bm).mgmtData;

		while(i < bufsize){
			if(PF[i].fixedPageCount != 0){
				// Page is Currently Being Used
				// Ignore
				// printf("Page is Being Used...Ignoring");
			}else{
				leastHitN = PF[i].leastRecentHit;
				leastHitI = i;
				break;
			}
			i++;
		}

		j = leastHitI + 1;
		while(j <= bufsize - 1){
			if(leastHitN < PF[j].leastRecentHit){
				// Not the least Hit
			}else{
				leastHitN = PF[j].leastRecentHit;
				leastHitI = j;
			}
			j++;
		}

		// Check For Page Modifications
		pgWriter(bm,PF,leastHitI);
		frameSetter(page,PF,leastHitI);
		// Update Hit
		PF[leastHitI].leastRecentHit = (*page).leastRecentHit;
	}
}

// CLOCK -> Last used clock stratergy implementation
// Inputs -> BufferPool, PageFrame
// Output -> None
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page){
	// Bufferpool & Page Checks
	if(buffer_pool_checks(bm) != RC_OK && page_frame_checks(page) != RC_OK){
		printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
	}else{
		// Both OK
		PF = (PageFrame *)(*bm).mgmtData;
		
		while(true){
			int temp = clkptr % bufsize;
			if(temp != 0){
				clkptr = clkptr;
			}
			else
				clkptr = 0;

			if(PF[clkptr].leastRecentHit == 0){
				pgWriter(bm,PF,clkptr);
				frameSetter(page,PF,clkptr);
				PF[clkptr].leastRecentHit = page->leastRecentHit;
				clkptr++;
				break;
			}if (PF[clkptr].leastRecentHit != 0){
				PF[clkptr++].leastRecentHit = 0;	
			}
		}
	}
}

// LFU -> LFU stratergy implementation
// Inputs -> BufferPool, PageFrame
// Output -> None
extern void LFU(BM_BufferPool *const bm, PageFrame *page){
	// Bufferpool & Page Checks
	if(buffer_pool_checks(bm) != RC_OK && page_frame_checks(page) != RC_OK){
		printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
	}else{
		// Both OK
		PF = (PageFrame *) (*bm).mgmtData;
		int i, j, leastFI = 0, leastFR = 0;
		i = j = 0;
		leastFI = lfuptr;

		while(i < bufsize){
			if(PF[leastFI].fixedPageCount != 0){
				// Page is used by someone
				// Ignoring
				// printf("Page Currently being used");
			}else{
				leastFR = PF[leastFI].leastFreqHit;
				leastFI = (leastFI + i) % bufsize;
				break;
			}
			i++;
		}

	i = (leastFI + 1) % bufsize;

		// Finding min
		while(j < bufsize){
			if(leastFR > PF[i].leastFreqHit){
				leastFI = i;
				leastFR = PF[i].leastFreqHit;
			}
			i = (i + 1) % bufsize;
			j++;
		}

		// Check for Page Modifications & then replace
		pgWriter(bm,PF,leastFI);
		frameSetter(page,PF,leastFI);
		lfuptr = leastFI + 1;
	}
}


// InitHelper -> Used by InitBufferPool
// Supporting Function
// Input -> Buffersize, PageFrame, Bufferpool
// Output -> RC_CODE
extern RC initHelper(int bufsize, PageFrame *page, BM_BufferPool *const bm){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	return_code = buffer_pool_checks(bm);
	// Bufferpool & Page Checks
	if(return_code != RC_OK && page_frame_checks(page) != RC_OK){
		// Error in Either One
		// Can't go ahead
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Both OK
		int i = 0;
		wrtct = lfuptr = clkptr = 0;
		while(i < bufsize){
			page[i].pageNum = -1;
			page[i].data = NULL;
			page[i].leastFreqHit = page[i].leastRecentHit = page[i].IfPageDirty = page[i].fixedPageCount = 0;
			i++;
		}
		(*bm).mgmtData = page;
	}
	return return_code;
}

// initBufferPool -> Initialize the BufferPool
// Inputs -> Bufferpool, Filename, no.of Pages, stratergy, stratData
// Output -> RC Code
extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = buffer_pool_checks(bm);
	// Bufferpool Checks
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = malloc(sizeof(PageFrame) * numPages);
		(*bm).numPages = numPages;
		(*bm).pageFile = (char *)pageFileName;
		(*bm).strategy = strategy;
		bufsize = numPages;
		initHelper(bufsize, PF, bm); 
	}
	return return_code;
}

extern RC CheckPinnedPages(int bufsize, PageFrame *pageFrame){	
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code = RC_OK;

	int i = 0;
	while(i < bufsize){
		if(pageFrame[i].fixedPageCount == 0)
			// Page is Currently being used can't do anything
			// printf("Page Currently Being Used");
			continue;
		else
			// Page not being used
			return_code = RC_PINNED_PAGES_IN_BUFFER;
			break;
		i++;
	}
	return return_code;
}

// shutdownBufferPool -> Closes the bufferpool
// Input -> BufferPool
// Output -> RC Code
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{	
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code, Output;

	return_code = buffer_pool_checks(bm);
	// Bufferpool Checks
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		forceFlushPool(bm);

		Output = CheckPinnedPages(bufsize, PF);
		if(Output == RC_OK){
			free(PF);
			(*bm).mgmtData = NULL;
			return_code = RC_OK;
		}
	}

	return Output;
} 

// flushPoolHelper -> Helper Function for forceFlushPool
// Input -> PageFrame, BufferPool
// Output -> RC Code
extern RC flushPoolHelper(PageFrame *pageFrame, BM_BufferPool *const bm, int i){
	RC return_code;

	//check if content of page is modified if so return error
	if(pageFrame[i].IfPageDirty != 1){
		return_code = RC_PAGE_DIRTY_FLAG_ERROR;
	}

	//check if page is accessed by someone if so return error
	else if(pageFrame[i].fixedPageCount != 0){
		return_code = RC_PAGE_FIX_COUNT_ERROR;
	}

	else if(pageFrame[i].IfPageDirty == 1){
		if(pageFrame[i].fixedPageCount != 0){
			return_code = RC_PAGE_FIX_COUNT_ERROR;
		}
		else{
			openPageFile((*bm).pageFile, &FH);
			writeBlock(pageFrame[i].pageNum, &FH, pageFrame[i].data);
			pageFrame[i].IfPageDirty = 0;
			wrtct++;
			return_code = RC_OK;
		}
	}
	else{
		return_code = RC_ERROR;
	}
	return return_code;
}

// forceFlushPool -> Writes Page to memory
// Input -> BufferPool
// Output -> RC Code
extern RC forceFlushPool(BM_BufferPool *const bm){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = buffer_pool_checks(bm);
	// Bufferpool Checks
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		int i=0;
		while(i < bufsize){
			flushPoolHelper(PF,bm,i);
			i++;
		}
		return_code = RC_OK;
	}
	return return_code;
}

// PageFound -> Finding Page Function
// Input -> PageFrame, PageHandle
// Output -> Boolean value
extern bool PageFound(PageFrame *pageFrame, BM_PageHandle *const page, int i){
	// Checks for PageFrame
	if(page_frame_checks(pageFrame) != RC_OK){
		// Page Error
		// printf("Page Error Found");
		// Code Execution would stop
	}else{
		if((*page).pageNum == pageFrame[i].pageNum)
			return true;
	}
	return false;
}

// PageFound -> Marks Page Dirty using PageFound helper function
// Input -> BufferPool, PageHandle
// Output -> RC Code
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	RC return_code;

	return_code = buffer_pool_checks(bm);
	// Bufferpool Checks
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		int i = 0;
		while(i < bufsize){
			if(PageFound(PF, page, i)){
				PF[i].IfPageDirty = 1;
				return RC_OK;		
			}
			i++;		
		}
	}

	return return_code;
}

// UnPinPage -> Removes Page from Memory
// Input -> BufferPool, PageHandle
// Output -> RC Code
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		int i = 0;
		while(i < bufsize){
			if(PageFound(PF, page, i)){
				PF[i].fixedPageCount--;
				break;		
			}
			i++;		
		}
		return_code = RC_OK;
	}	
	
	return return_code;
}

// ForcePage -> Write Modified Page to disk
// Input -> BufferPool, PageHandle
// Output -> RC Code
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		int i = 0;
		while(i < bufsize){
			// Find the page
			if(PageFound(PF, page, i)){		
				openPageFile((*bm).pageFile, &FH);
				writeBlock(PF[i].pageNum, &FH, PF[i].data);
				PF[i].IfPageDirty = 0;
				wrtct++;
			}
			i++;
		}
		return_code = RC_OK;
	}

	return return_code;
}

// InsertInBuffer -> Insert Page in Buffer
// Input -> Bufferpool, PageHandle, Pageno, PageFrame
// Output -> None
extern void InsertinBuffer(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum, PageFrame *pageFrame,int i){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		// return return_code;
	}else{
		// Everything OK
		openPageFile((*bm).pageFile, &FH);
		pageFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&FH);
		readBlock(pageNum, &FH, pageFrame[i].data);
		
		pageFrame[i].leastFreqHit = 0;
		pageFrame[i].pageNum = pageNum;

		if(i != 0){
			ridx++;	
			hitctr++;
			pageFrame[i].fixedPageCount = 1;
		}else{
			ridx = hitctr = 0;
			pageFrame[i].leastRecentHit = hitctr;
			pageFrame[i].fixedPageCount++;
		}

		(*page).data = pageFrame[i].data;
		(*page).pageNum = pageNum;
	}
}

// ImplementReplacementStrategy -> Helper for pinPage
// Inputs -> BufferPool, PageHandle
// Output -> None (Redirection to Strategy)
extern void ImplementReplacementStrategy(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		// return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *) malloc(sizeof(PageFrame));
		openPageFile((*bm).pageFile, &FH);
		(*PF).data = (SM_PageHandle) malloc(PAGE_SIZE);
		readBlock(pageNum, &FH, (*PF).data);
		(*PF).IfPageDirty = 0;	
		(*PF).fixedPageCount = 1;
		(*PF).leastFreqHit = 0;
		(*PF).pageNum = pageNum;
		ridx++;
		hitctr++;
		(*page).pageNum = pageNum;
		(*page).data = (*PF).data;

		// Update PageFrame based on Strategy
		if((*bm).strategy == RS_LRU)
			(*PF).leastRecentHit = hitctr;				
		else if((*bm).strategy == RS_CLOCK)
			(*PF).leastRecentHit = 1;

		// Deligate based on strategy
		if((*bm).strategy == RS_FIFO){
			FIFO(bm, PF);
		}else if((*bm).strategy == RS_CLOCK){
			CLOCK(bm, PF);
		}else if((*bm).strategy == RS_LFU){
			LFU(bm, PF);
		}else if((*bm).strategy == RS_LRU){
			LRU(bm, PF);
		}else{
			printf("No Strategy");
		}
	}
}

// PinPage -> Add Page to Buffer Pool using helper functions
// Inputs -> BufferPool, PageHandle, PageNum
// Outputs -> RC Code
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// printf("Buffer Pool Init Error: %d", RC_BUFFER_POOL_INIT);
		return return_code;
	}else{
		// Everything OK
		PF = (PageFrame *)(*bm).mgmtData;
		
		// Check Bufferpool
		if(PF[0].pageNum != -1){
			// Page Exist in Bufferpool
			// Assuming buffer is Full
			bool checkBufFull = true;

			int i=0;
			while(i < bufsize){
				if(PF[i].pageNum == -1){
					// Meaning Page Not in Buf
					// Bring to Buf using Helper file
					InsertinBuffer(bm, page, pageNum, PF, i);
					(*page).data = PF[i].data;
					(*page).pageNum = pageNum;

					if((*bm).strategy == RS_CLOCK)
						PF[i].leastRecentHit = 1;
					else if((*bm).strategy == RS_LRU)
						PF[i].leastRecentHit = hitctr;
					
					checkBufFull = false;
					break;
				}else{
					// Page in Buf
					// PF[i].pageNum != -1
					if(PF[i].pageNum != pageNum){
						// Buff & Mem not matching
						// printf("Buff Not Matching");
					}
					else{
						PF[i].fixedPageCount++;
						checkBufFull = false;
						hitctr++;
						(*page).data = PF[i].data;
						(*page).pageNum = pageNum;

						if((*bm).strategy == RS_CLOCK)
							PF[i].leastRecentHit = 1;
						else if((*bm).strategy == RS_LFU)
							PF[i].leastFreqHit++;
						else if((*bm).strategy == RS_LRU)
							PF[i].leastRecentHit = hitctr;
						
						clkptr++;
						break;
					}
				}
				i++;
			}

			if(checkBufFull == true){
				ImplementReplacementStrategy(bm,page,pageNum);
			}
			return RC_OK;
		}else{
			// No Page Exist
			// Read page and return
			InsertinBuffer(bm, page, pageNum, PF, 0);
			return RC_OK;
		}
				
	}
}

/* 
getFrameContents 
Defined -> buffer_mgr.h
Input -> BufferPool
Output -> PageNumber Array 
*/
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	PageNumber *FC = malloc(sizeof(PageNumber) * bufsize);

	// First is to check if Buffer Pool Exist
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// Buffer Pool Error
		return FC;
	}else{
		// Everything is OK
		PF = (PageFrame *) (*bm).mgmtData;

		int i=0;
		while(i<bufsize){
			if(PF[i].pageNum < 0)
				FC[i] = -1;
			else
				FC[i] = PF[i].pageNum;
			i++;
		}
		return FC;
	}
}

/* 
getDirtyFlags 
Defined -> buffer_mgr.h
Input -> BufferPool
Output -> Boolean Array 
*/
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	// Blank Alloc so as to return empty in case there are any errors
	bool *drtyFlgs = malloc(sizeof(bool) * bufsize);
	
	// First is to check if Buffer Pool Exist
	return_code = buffer_pool_checks(bm);

	if(return_code != RC_OK){
		// Buffer Pool Error
		// Return Empty alloctment
		return drtyFlgs;
	}else{
		// Everything is OKAY
		PF = (PageFrame *)(*bm).mgmtData;
		
		// Check for dirtyFlags
		int i=0;
		while(i<bufsize){
			// Check the BIT
			if(PF[i].IfPageDirty == 0){
				drtyFlgs[i] = false;
			}else
				drtyFlgs[i] = true;
			i++;
		}

		return drtyFlgs;
	}
}

/* 
getFixCounts 
Defined -> buffer_mgr.h
Input -> BufferPool
Output -> Integer Array 
*/
extern int *getFixCounts (BM_BufferPool *const bm)
{
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;
	// Blank Alloc so as to return empty in case there are any errors
	int *fixCnts = malloc(sizeof(int) * bufsize);

	// First is to check if Buffer Pool Exist
	// If Not there is no point going further
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK){
		// Return empty alloctment as return code error
		return fixCnts;
	}else{
		// Return code returned RC_OK
		
		PF = (PageFrame *)(*bm).mgmtData;
		
		int i=0;

		while(i < bufsize){
			if(PF[i].fixedPageCount > 0){
				fixCnts[i] = PF[i].fixedPageCount;
			}else{
				fixCnts[i] = 0;
			}
			i++;
		}

		// Check with return_code if RC_OK then return else return error
		if(return_code == RC_OK)
			return fixCnts;
		else
			return fixCnts;
	}

}

/* 
getNumReadIO 
Defined -> buffer_mgr.h
Input -> BufferPool
Output -> Read no. of Pages
*/
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// First is to check if Buffer Pool Exist
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK)
		return return_code;
	else
		// Index starts with 0
		return (ridx + 1);
}

/* 
getNumWriteIO 
Defined -> buffer_mgr.h
Input -> BufferPool
Output -> Total no. of Pages 
*/
extern int getNumWriteIO (BM_BufferPool *const bm){
	// RC is already defined in dberror.h file using it to store RC_CODES
	RC return_code;

	// First is to check if Buffer Pool Exist
	return_code = buffer_pool_checks(bm);
	if(return_code != RC_OK)
		return return_code;
	else
		return wrtct;
}