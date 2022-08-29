#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_ERROR 400 // Added a new definiton for ERROR

// Custom File Operation Erros
#define RC_FILE_CLOSING_ERROR 500
#define RC_FILE_DESTROY_ERROR 501
#define RC_FILE_MGMT_INFO_ERROR 502
#define RC_FILE_READ_ERROR 503

// Buffer related Erros
#define RC_BUFFER_POOL_INIT 600
#define RC_PINNED_PAGES_IN_BUFFER 601
#define RC_BUFFER_POOL_MGMT_DATA_INIT 602

#define RC_PAGE_NOT_FOUND 700
#define RC_PAGE_DIRTY_FLAG_ERROR 701
#define RC_PAGE_FIX_COUNT_ERROR 702
#define RC_PAGE_HIT_ERROR 703
#define RC_PAGE_REF_NUM_ERROR 704
#define RC_PAGE_INIT 705
#define RC_PAGE_DATA_ERROR 706
#define RC_PAGE_NO_ERROR 707
#define RC_PAGE_CONCURENT_ERROR 708
#define RC_PAGE_IN_USE 709

#define RC_SCHEMA_DATA_TYPE_ERROR 800
#define RC_RECORD_NULL_ERROR 801
#define RC_SCHEMA_NULL_ERROR 802
#define RC_VALUE_NULL_ERROR 803
#define RC_RELATION_NULL_ERROR 804
#define RC_RID_ERROR 805

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 206

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

#define RC_SCAN_CONDITION_NOT_FOUND 401
#define RC_SCAN_NOT_FOUND 402
#define RC_SCAN_COM_ERROR 403
#define RC_CONDITION_NOT_MET 404

#define RC_PIN_PAGE_FAILED 900
#define RC_UNPIN_PAGE_FAILED 901
#define RC_BUFFER_SHUTDOWN_FAILED 902
#define RC_NULL_IP_PARAM 903
#define RC_FILE_DESTROY_FAILED 904
#define RC_FORCE_PAGE_FAILED 905


/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
		do {			  \
			RC_message=message;	  \
			return rc;		  \
		} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
		do {									\
			int rc_internal = (code);						\
			if (rc_internal != RC_OK)						\
			{									\
				char *message = errorMessage(rc_internal);			\
				printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
				free(message);							\
				exit(1);							\
			}									\
		} while(0);


#endif
